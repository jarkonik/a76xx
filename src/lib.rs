#![no_std]

use core::any::type_name;
use core::fmt;

use atat::asynch::{AtatClient, Client};
use atat::atat_derive::{AtatCmd, AtatEnum, AtatResp, AtatUrc};
use atat::digest::parser::take_until_including;
use atat::heapless::String;
use atat::nom::IResult;
use atat::serde_at::serde::de::Visitor;
use atat::serde_at::serde::{self, de};
use atat::serde_at::serde::{Deserialize, Deserializer};
use atat::{AtatCmd, nom};
#[cfg(feature = "defmt")]
use defmt::{Format, write};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::pubsub::{Subscriber, WaitResult};
use embassy_time::{Duration, Timer, with_timeout};
use embedded_hal::digital::OutputPin;

mod transport;

pub use transport::{ModemResources, PppIo, RxPump, TxPump};

const AT_TX_PIPE_SIZE: usize = 128;
const AT_TX_BUF_SIZE: usize = 128;
const INGRESS_BUF_SIZE: usize = 128;
const URC_CAPACITY: usize = 128;
const URC_SUBSCRIBERS: usize = 3;

#[cfg_attr(feature = "defmt", derive(Format))]
#[derive(Debug)]
pub enum Error {
    #[cfg_attr(feature = "defmt", defmt(Debug2Format))]
    PppAlreadyTaken,
    #[cfg_attr(feature = "defmt", defmt(Debug2Format))]
    WaitForUrcError,
    #[cfg_attr(feature = "defmt", defmt(Debug2Format))]
    PowerOnError,
    #[cfg_attr(feature = "defmt", defmt(Debug2Format))]
    AtCommandError {
        cmd: &'static str,
        source: atat::Error,
    },
}

fn trim_ascii_whitespace(x: &[u8]) -> &[u8] {
    let from = match x.iter().position(|x| !x.is_ascii_whitespace()) {
        Some(i) => i,
        None => return &x[0..0],
    };
    let to = x.iter().rposition(|x| !x.is_ascii_whitespace()).unwrap();
    &x[from..=to]
}

type ParseResult<'a> = IResult<&'a [u8], (&'a [u8], usize), nom::error::Error<&'a [u8]>>;

fn parse_http_head(_data: &[u8]) -> impl Fn(&[u8]) -> ParseResult {
    move |input| {
        let (i, (le, urc_tag)) = nom::sequence::tuple((
            nom::character::complete::line_ending,
            nom::combinator::recognize(nom::sequence::tuple((
                nom::bytes::streaming::tag("+HTTPHEAD"),
                nom::bytes::streaming::tag(":"),
                take_until_including("\r\n"),
            ))),
        ))(input)?;

        let idx = urc_tag.iter().position(|&b| b == b' ').unwrap();
        let (_tag, body_len_string) = urc_tag.split_at(idx);
        let body_len: usize = core::str::from_utf8(trim_ascii_whitespace(body_len_string))
            .unwrap()
            .parse()
            .unwrap();
        let (i, body) = nom::bytes::complete::take(body_len)(i)?;

        Ok((
            i,
            (
                trim_ascii_whitespace(&input[0..(le.len() + urc_tag.len() + body.len())]),
                le.len() + urc_tag.len() + body.len(),
            ),
        ))
    }
}

#[cfg_attr(feature = "defmt", derive(Format))]
#[derive(Clone, Debug, AtatUrc)]
pub enum Urc {
    #[at_urc("*ATREADY")]
    ATREADY(RawResponse),
    #[at_urc("+CPIN")]
    CPIN(RawResponse),
    #[at_urc("+CEREG")]
    CEREG(RawResponse),
    #[at_urc("+CGEV")]
    CGEV(RawResponse),
    #[at_urc("+NETOPEN")]
    NETOPEN(RawResponse),
    #[at_urc("+HTTPACTION")]
    HTTPACTION(RawResponse),
    #[at_urc("+CPSI")]
    CPSI(CPSIResponse),
    #[at_urc("+HTTPHEAD", parse = parse_http_head)]
    HTTPHEAD(HttpHeadResponse),
    #[at_urc("SMS DONE")]
    SMSDONE(RawResponse),
}

pub struct Modem<'a, P, const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize> {
    power: P,
    client: Client<'a, transport::AtWriter<'a, AT_TX_PIPE_SIZE>, INGRESS_BUF_SIZE>,
    subscription: Subscriber<'a, CriticalSectionRawMutex, Urc, URC_CAPACITY, URC_SUBSCRIBERS, 1>,
    mode: &'a transport::DataMode,
    ppp_io: Option<PppIo<'a, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>>,
}

pub struct PowerPins<P> {
    pub power_control: P,
    pub reset: P,
    pub dtr: P,
    pub power_on: P,
}

pub trait ModemPower {
    fn power_on(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

impl<P: OutputPin + Send> ModemPower for PowerPins<P> {
    async fn power_on(&mut self) -> Result<(), Error> {
        #[cfg(feature = "defmt")]
        defmt::trace!("Powering on modem");

        self.power_control.set_high().unwrap();

        self.reset.set_low().unwrap();
        Timer::after(Duration::from_millis(100)).await;
        self.reset.set_high().unwrap();
        Timer::after(Duration::from_millis(2600)).await;
        self.reset.set_low().unwrap();

        self.dtr.set_low().unwrap();

        self.power_on.set_low().unwrap();
        Timer::after(Duration::from_millis(100)).await;
        self.power_on.set_high().unwrap();
        Timer::after(Duration::from_millis(100)).await;
        self.power_on.set_low().unwrap();

        Ok(())
    }
}

#[derive(Clone, AtatResp)]
pub struct NoResponse;

#[derive(Clone, AtatCmd)]
#[at_cmd("", NoResponse, timeout_ms = 1000)]
pub struct AT;

#[derive(Clone, AtatCmd)]
#[at_cmd("E0", NoResponse, timeout_ms = 1000)]
pub struct E0;

#[derive(Clone, AtatCmd)]
#[at_cmd("+HTTPHEAD", NoResponse, timeout_ms = 5000)]
pub struct HTTPHEAD;

#[derive(Clone, AtatCmd)]
#[at_cmd("+IFC", NoResponse, timeout_ms = 1000)]
pub struct IFC {
    #[at_arg(position = 0)]
    pub by_te: u8,
    #[at_arg(position = 1)]
    pub by_dce: u8,
}

#[derive(Clone, AtatCmd)]
#[at_cmd(
    "ATD",
    RawResponse,
    timeout_ms = 30000,
    cmd_prefix = "",
    value_sep = false,
    quote_escape_strings = false
)]
pub struct ATD<'a> {
    #[at_arg(position = 0, len = 32)]
    number: &'a str,
}

#[derive(AtatEnum, Clone)]
pub enum HttpAction {
    Get,
}

#[derive(Clone, AtatCmd)]
#[at_cmd("+HTTPACTION", NoResponse, timeout_ms = 1000)]
pub struct HTTPACTION {
    #[at_arg(position = 0)]
    action: HttpAction,
}

#[derive(Clone, AtatCmd)]
#[at_cmd("+CPIN", NoResponse, timeout_ms = 1000)]
pub struct CPIN<'a> {
    #[at_arg(position = 0, len = 8)]
    pin: &'a str,
}

#[derive(Clone, AtatCmd)]
#[at_cmd("+HTTPPARA", NoResponse, timeout_ms = 1000)]
pub struct HTTPPARA<'a> {
    #[at_arg(position = 0, len = 8)]
    param_name: &'a str,
    #[at_arg(position = 0, len = 1024)]
    param_value: &'a str,
}

#[derive(Clone, AtatCmd)]
#[at_cmd("+CEREG?", NoResponse, timeout_ms = 120000)]
pub struct CEREGQuery;

#[derive(Clone, AtatCmd)]
#[at_cmd("+CPSI?", NoResponse, timeout_ms = 120000)]
pub struct CPSIQuery;

#[derive(Clone, AtatCmd)]
#[at_cmd("+IPR", NoResponse, timeout_ms = 1000)]
pub struct IPR {
    #[at_arg(position = 0)]
    rate: u32,
}

#[derive(Clone, AtatCmd)]
#[at_cmd("+HTTPINIT", NoResponse, timeout_ms = 120000)]
pub struct HTTPINIT;

#[derive(Clone, AtatCmd)]
#[at_cmd("+NETOPEN", NoResponse, timeout_ms = 120000)]
pub struct NETOPEN;

#[cfg_attr(feature = "defmt", derive(Format))]
#[derive(Clone, Debug, AtatResp)]
pub struct RawResponse {
    pub resp: String<256>,
}

#[cfg_attr(feature = "defmt", derive(Format))]
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SystemMode {
    NoService,
    GSM,
    WCDMA,
    LTE,
}

struct ModeVisitor;

impl<'de> Visitor<'de> for ModeVisitor {
    type Value = SystemMode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("system mode string")
    }

    fn visit_bytes<E>(self, v: &'_ [u8]) -> Result<<ModeVisitor as Visitor<'_>>::Value, E>
    where
        E: de::Error,
    {
        match v {
            b"NO SERVICE" => Ok(SystemMode::NoService),
            b"GSM" => Ok(SystemMode::GSM),
            b"WCDMA" => Ok(SystemMode::WCDMA),
            b"LTE" => Ok(SystemMode::LTE),
            _ => Err(de::Error::custom("incorrect mode")),
        }
    }
}

impl<'de> Deserialize<'de> for SystemMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(ModeVisitor)
    }
}

#[cfg_attr(feature = "defmt", derive(Format))]
#[derive(Clone, Debug, AtatResp)]
pub struct CPSIResponse {
    #[at_arg(position = 0)]
    pub system_mode: SystemMode,
    #[at_arg(position = 1)]
    pub operation_mode: String<256>,
}

#[derive(Clone, Debug)]
pub struct LengthDelimited<const N: usize, const S: usize = 1> {
    /// The number of bytes in the payload. This is actually
    /// redundant since the `bytes` field also knows its own length.
    pub len: usize,
    /// The payload bytes
    pub bytes: heapless::Vec<u8, N>,
}

impl<'de, const N: usize, const S: usize> Deserialize<'de> for LengthDelimited<N, S> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Ideally we use deserializer.deserialize_bytes but since it clips the payload
        // at the first comma we cannot use it.
        // Instead we use deserialize_tuple as it wasn't used yet.
        deserializer.deserialize_tuple(2, LengthDelimitedVisitor::<N, S>) // The '2' is dummy.
    }
}

struct LengthDelimitedVisitor<const N: usize, const L: usize>;

impl<'de, const N: usize, const S: usize> serde::de::Visitor<'de> for LengthDelimitedVisitor<N, S> {
    type Value = LengthDelimited<N, S>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("length delimited bytes, e.g.: \"4,ABCD\"")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.iter()
            .position(|&c| !c.is_ascii_digit())
            .ok_or_else(|| de::Error::custom("expected a comma"))
            .and_then(|pos| {
                let len = parse_len(&v[0..pos])
                    .map_err(|_| de::Error::custom("expected an unsigned int"))?;
                // +S to skip the separator after the length.
                let mut start = pos + S;
                let mut end = start + len - S;
                // Check if payload is surrounded by double quotes not included in len.
                let slice_len = v.len();
                if slice_len >= (end + 2) && (v[start] == b'"' && v[end + 1] == b'"') {
                    start += 1; // Extra +1 to remove first quote (")
                    end += 1; // Move end by 1 to compensate for the quote.
                }
                Ok(LengthDelimited {
                    len,
                    bytes: heapless::Vec::try_from(&v[start..end])
                        .map_err(|_| de::Error::custom("incorrect slice size"))?,
                })
            })
    }
}

/// Parses a slice of bytes into an unsigned integer.
/// The slice must contain only ASCII _digits_ and must not contain additional bytes.
fn parse_len(v: &[u8]) -> Result<usize, ()> {
    let len_str: &str = core::str::from_utf8(v).map_err(|_| ())?;
    len_str.parse().map_err(|_| ())
}

#[derive(Clone, Debug, AtatResp)]
pub struct HttpHeadResponse {
    #[at_arg(position = 0)]
    pub body: LengthDelimited<512, 2>,
}

#[cfg(feature = "defmt")]
impl Format for HttpHeadResponse {
    fn format(&self, fmt: defmt::Formatter) {
        write!(fmt, "HttpHeadResponse({:?})", &self.body.bytes[..]);
    }
}

#[derive(Clone, AtatCmd)]
#[at_cmd("+CPIN?", NoResponse, timeout_ms = 1000)]
pub struct CPINQuery;

#[derive(Clone, AtatCmd)]
#[at_cmd("+CGDCONT", NoResponse, timeout_ms = 1000)]
pub struct CGDCONT<'a> {
    #[at_arg(position = 0, len = 2)]
    cid: u8,
    #[at_arg(position = 1, len = 6)]
    pdp_type: &'a str,
    #[at_arg(position = 2, len = 32)]
    apn: &'a str,
}

impl<'a, P: ModemPower + Send, const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize>
    Modem<'a, P, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>
{
    pub fn new<const TX_BUF_SIZE: usize, const RX_BUF_SIZE: usize>(
        resources: &'a mut transport::ModemResources<
            DATA_TX_PIPE_SIZE,
            DATA_RX_PIPE_SIZE,
            TX_BUF_SIZE,
            RX_BUF_SIZE,
        >,
        power: P,
    ) -> (
        Self,
        transport::RxPump<'a, DATA_RX_PIPE_SIZE, RX_BUF_SIZE>,
        transport::TxPump<'a, DATA_TX_PIPE_SIZE, TX_BUF_SIZE>,
    ) {
        let (mode, ingress_res_slot, urc_channel, writer, rx_pump, tx_pump, ppp_io, at_tx_buf) =
            resources.split();
        let client = Client::new(writer, ingress_res_slot, at_tx_buf, atat::Config::default());
        let subscription = urc_channel.subscribe().unwrap();

        (
            Modem {
                power,
                client,
                subscription,
                mode,
                ppp_io: Some(ppp_io),
            },
            rx_pump,
            tx_pump,
        )
    }

    pub async fn power_on(&mut self) -> Result<(), Error> {
        self.power.power_on().await
    }

    async fn send_cmd<Cmd: AtatCmd>(&mut self, cmd: &Cmd) -> Result<Cmd::Response, Error> {
        self.client
            .send(cmd)
            .await
            .map_err(|e| Error::AtCommandError {
                cmd: type_name::<Cmd>(),
                source: e,
            })
    }

    pub async fn sim_status(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&CPINQuery).await
    }

    pub async fn set_pin(&mut self, pin: &str) -> Result<NoResponse, Error> {
        self.send_cmd(&CPIN { pin }).await
    }

    pub async fn wait_for_connection(&mut self) {
        loop {
            if self.client.send(&AT).await.is_ok() {
                #[cfg(feature = "defmt")]
                defmt::trace!("Received ok response");
                break;
            };
            Timer::after(Duration::from_millis(100)).await;
        }
    }

    pub async fn request_baudrate(&mut self, rate: u32) -> Result<NoResponse, Error> {
        self.send_cmd(&IPR { rate }).await
    }

    pub async fn disable_echo(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&E0).await
    }

    pub async fn open_network(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&NETOPEN).await
    }

    pub async fn check_registration(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&CEREGQuery).await
    }

    pub async fn check_cpsi(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&CPSIQuery).await
    }

    pub async fn set_http_action(&mut self, action: HttpAction) -> Result<NoResponse, Error> {
        self.send_cmd(&HTTPACTION { action }).await
    }

    pub async fn init_http(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&HTTPINIT).await
    }

    pub async fn set_http_param(
        &mut self,
        param_name: &str,
        param_value: &str,
    ) -> Result<NoResponse, Error> {
        self.send_cmd(&HTTPPARA {
            param_name,
            param_value,
        })
        .await
    }

    pub async fn head_http(&mut self) -> Result<NoResponse, Error> {
        self.send_cmd(&HTTPHEAD).await
    }

    pub async fn set_flow_control(&mut self, by_te: u8, by_dce: u8) -> Result<NoResponse, Error> {
        self.send_cmd(&IFC { by_te, by_dce }).await
    }

    pub async fn dial(&mut self, number: &str) -> Result<RawResponse, Error> {
        self.send_cmd(&ATD { number }).await
    }

    pub async fn define_pdp_context(
        &mut self,
        cid: u8,
        pdp_type: &str,
        apn: &str,
    ) -> Result<NoResponse, Error> {
        self.send_cmd(&CGDCONT { cid, pdp_type, apn }).await
    }

    pub async fn wait_for_sim(&mut self) {
        loop {
            if self.sim_status().await.is_ok() {
                break;
            }
            Timer::after(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_for_registration(&mut self) {
        loop {
            if self.check_registration().await.is_ok() {
                break;
            }
            Timer::after(Duration::from_millis(100)).await;
        }
    }

    async fn wait_for_urc<F, T>(&mut self, mut f: F, timeout: Duration) -> Result<T, Error>
    where
        F: FnMut(Urc) -> Option<T>,
    {
        with_timeout(timeout, async {
            loop {
                let msg = self.subscription.next_message().await;

                if let WaitResult::Message(msg) = msg
                    && let Some(val) = f(msg)
                {
                    return Ok(val);
                }
            }
        })
        .await
        .map_err(|_| Error::WaitForUrcError)?
    }

    pub async fn wait_for_service(&mut self) -> Result<(), Error> {
        loop {
            self.check_cpsi().await?;

            let cpsi = self
                .wait_for_urc(
                    |urc| match urc {
                        Urc::CPSI(cpsi) => Some(cpsi),
                        _ => None,
                    },
                    Duration::from_secs(60),
                )
                .await?;

            if cpsi.system_mode != SystemMode::NoService {
                return Ok(());
            }
        }
    }

    async fn prepare_ppp(&mut self, pin: &str, apn: &str) -> Result<(), Error> {
        self.wait_for_connection().await;
        self.disable_echo().await?;
        self.wait_for_sim().await;
        self.set_pin(pin).await?;
        self.wait_for_registration().await;
        self.wait_for_service().await?;
        self.define_pdp_context(1, "IP", apn).await?;

        Ok(())
    }

    pub async fn connect_ppp<'b>(
        &mut self,
        pin: &str,
        apn: &str,
        number: &str,
    ) -> Result<PppIo<'b, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>, Error>
    where
        'a: 'b,
    {
        let ppp_io = self.ppp_io.take().ok_or(Error::PppAlreadyTaken)?;
        self.prepare_ppp(pin, apn).await?;
        self.dial(number).await?;
        self.mode.enter_data_mode();

        Ok(ppp_io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_httphead_urc() {
        let original_input = concat!(
            "\r\n+HTTPHEAD: 67\r\n",
            "HTTP/1.1 301 Moved Permanently\r\n",
            "Location: https://www.example.com\r\n"
        );

        let (input, (output, len)) =
            parse_http_head(original_input.as_bytes())(original_input.as_bytes()).unwrap();
        assert_eq!(
            str::from_utf8(output).unwrap(),
            "+HTTPHEAD: 67\r\nHTTP/1.1 301 Moved Permanently\r\nLocation: https://www.example.com"
        );
        assert_eq!(str::from_utf8(input).unwrap(), "");
        assert_eq!(len, 84);
    }

    #[test]
    fn it_deserializes_httphead_urc() {
        let urc =
            b"+HTTPHEAD: 67\r\nHTTP/1.1 301 Moved Permanently\r\nLocation: https://www.example.com";
        let _result: HttpHeadResponse = atat::serde_at::from_slice(urc).unwrap();
    }

    #[test]
    fn it_deserializes_cpsi_urc() {
        let urc = b"+CPSI: LTE,Online";
        let result: CPSIResponse = atat::serde_at::from_slice(urc).unwrap();
        assert_eq!(result.system_mode, SystemMode::LTE);
    }
}
