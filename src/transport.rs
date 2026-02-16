use core::fmt;
use core::sync::atomic::{AtomicU8, Ordering};

use atat::{AtatIngress as _, ResponseSlot, UrcChannel};
use embassy_futures::select::{Either, select};
use embassy_sync::blocking_mutex::raw::{CriticalSectionRawMutex, NoopRawMutex};
use embassy_sync::pipe::{Pipe, Reader, Writer};
use embassy_sync::watch::Watch;
use embedded_io_async::{BufRead, Read, Write};

use crate::{
    AT_TX_BUF_SIZE, AT_TX_PIPE_SIZE, INGRESS_BUF_SIZE, URC_CAPACITY, URC_SUBSCRIBERS, Urc,
};

#[repr(u8)]
enum Mode {
    AT = 0,
    Data = 1,
}

type Ingress<'a> = atat::Ingress<
    'a,
    atat::DefaultDigester<Urc>,
    Urc,
    INGRESS_BUF_SIZE,
    URC_CAPACITY,
    URC_SUBSCRIBERS,
>;

pub(crate) struct DataMode {
    mode: AtomicU8,
    watch: Watch<NoopRawMutex, (), 2>,
}

impl DataMode {
    const fn new_at() -> Self {
        Self {
            mode: AtomicU8::new(Mode::AT as u8),
            watch: Watch::new(),
        }
    }

    pub(crate) fn enter_data_mode(&self) {
        self.mode.store(Mode::Data as u8, Ordering::Release);
        self.watch.sender().send(());
    }

    fn is_data_mode(&self) -> bool {
        self.mode.load(Ordering::Acquire) == Mode::Data as u8
    }
}

pub(crate) type AtWriter<'a, const AT_TX_PIPE_SIZE: usize> =
    Writer<'a, CriticalSectionRawMutex, AT_TX_PIPE_SIZE>;

fn parse_connect_success(input: &[u8]) -> Result<(&[u8], usize), atat::digest::ParseError> {
    if let Some(start) = input.windows(2).position(|w| w == b"\r\n") {
        let after_crlf = &input[start + 2..];

        if after_crlf.starts_with(b"CONNECT")
            && let Some(end) = after_crlf.windows(2).position(|w| w == b"\r\n")
        {
            let total_len = start + 2 + end + 2;
            return Ok((&input[..total_len], total_len));
        }
    }

    Err(atat::digest::ParseError::NoMatch)
}

pub struct ModemResources<
    const DATA_TX_PIPE_SIZE: usize,
    const DATA_RX_PIPE_SIZE: usize,
    const TX_BUF_SIZE: usize,
    const RX_BUF_SIZE: usize,
> {
    mode: DataMode,
    at_tx_pipe: Pipe<CriticalSectionRawMutex, AT_TX_PIPE_SIZE>,
    data_tx_pipe: Pipe<CriticalSectionRawMutex, DATA_TX_PIPE_SIZE>,
    data_rx_pipe: Pipe<CriticalSectionRawMutex, DATA_RX_PIPE_SIZE>,
    ingress_buf: [u8; INGRESS_BUF_SIZE],
    res_slot: ResponseSlot<INGRESS_BUF_SIZE>,
    urc_channel: UrcChannel<Urc, URC_CAPACITY, URC_SUBSCRIBERS>,
    at_tx_buf: [u8; AT_TX_BUF_SIZE],
}

impl<
    const DATA_TX_PIPE_SIZE: usize,
    const DATA_RX_PIPE_SIZE: usize,
    const TX_BUF_SIZE: usize,
    const RX_BUF_SIZE: usize,
> ModemResources<DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE, TX_BUF_SIZE, RX_BUF_SIZE>
{
    pub const fn new() -> Self {
        Self {
            mode: DataMode::new_at(),
            at_tx_pipe: Pipe::new(),
            data_tx_pipe: Pipe::new(),
            data_rx_pipe: Pipe::new(),
            ingress_buf: [0; INGRESS_BUF_SIZE],
            res_slot: ResponseSlot::new(),
            urc_channel: UrcChannel::new(),
            at_tx_buf: [0; AT_TX_BUF_SIZE],
        }
    }

    pub(crate) fn split(
        &mut self,
    ) -> (
        &'_ DataMode,
        &'_ ResponseSlot<INGRESS_BUF_SIZE>,
        &'_ UrcChannel<Urc, URC_CAPACITY, URC_SUBSCRIBERS>,
        AtWriter<'_, AT_TX_PIPE_SIZE>,
        RxPump<'_, DATA_RX_PIPE_SIZE, RX_BUF_SIZE>,
        TxPump<'_, DATA_TX_PIPE_SIZE, TX_BUF_SIZE>,
        PppIo<'_, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>,
        &'_ mut [u8; AT_TX_BUF_SIZE],
    ) {
        let mode = &self.mode;
        let (at_tx_reader, at_tx_writer) = self.at_tx_pipe.split();
        let (data_tx_reader, data_tx_writer) = self.data_tx_pipe.split();
        let (data_rx_reader, data_rx_writer) = self.data_rx_pipe.split();

        let ingress = atat::Ingress::new(
            atat::DefaultDigester::<Urc>::default().with_custom_success(parse_connect_success),
            &mut self.ingress_buf,
            &self.res_slot,
            &self.urc_channel,
        );

        (
            &self.mode,
            &self.res_slot,
            &self.urc_channel,
            at_tx_writer,
            RxPump {
                data_rx_writer,
                ingress,
                mode,
            },
            TxPump {
                at_tx_reader,
                data_tx_reader,
                mode,
            },
            PppIo::new(data_tx_writer, data_rx_reader),
            &mut self.at_tx_buf,
        )
    }
}

impl<
    const DATA_TX_PIPE_SIZE: usize,
    const DATA_RX_PIPE_SIZE: usize,
    const TX_BUF_SIZE: usize,
    const RX_BUF_SIZE: usize,
> Default for ModemResources<DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE, TX_BUF_SIZE, RX_BUF_SIZE>
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct TxPump<'a, const DATA_TX_PIPE_SIZE: usize, const TX_BUF_SIZE: usize> {
    at_tx_reader: Reader<'a, CriticalSectionRawMutex, AT_TX_PIPE_SIZE>,
    data_tx_reader: Reader<'a, CriticalSectionRawMutex, DATA_TX_PIPE_SIZE>,
    mode: &'a DataMode,
}

impl<const DATA_TX_PIPE_SIZE: usize, const TX_BUF_SIZE: usize>
    TxPump<'_, DATA_TX_PIPE_SIZE, TX_BUF_SIZE>
{
    pub async fn run<W>(self, mut uart_tx: W) -> !
    where
        W: Write,
    {
        let mut buf = [0u8; TX_BUF_SIZE];
        let at_tx_reader = self.at_tx_reader;
        let data_tx_reader = self.data_tx_reader;

        let mut receiver = self.mode.watch.receiver().unwrap();

        loop {
            let is_data_mode = self.mode.is_data_mode();
            let n = if is_data_mode {
                match select(data_tx_reader.read(&mut buf), receiver.changed()).await {
                    Either::First(n) => n,
                    Either::Second(_) => continue,
                }
            } else {
                match select(at_tx_reader.read(&mut buf), receiver.changed()).await {
                    Either::First(n) => n,
                    Either::Second(_) => continue,
                }
            };
            #[cfg(feature = "defmt")]
            defmt::trace!(
                "[{}][TX] dequeued {} bytes",
                if is_data_mode { "Data" } else { "AT" },
                n
            );
            uart_tx.write_all(&buf[..n]).await.unwrap();
            #[cfg(feature = "defmt")]
            defmt::trace!(
                "[{}][TX] sent {} bytes",
                if is_data_mode { "Data" } else { "AT" },
                n
            );
        }
    }
}

pub struct RxPump<'a, const DATA_RX_PIPE_SIZE: usize, const RX_BUF_SIZE: usize> {
    data_rx_writer: Writer<'a, CriticalSectionRawMutex, DATA_RX_PIPE_SIZE>,
    ingress: Ingress<'a>,
    mode: &'a DataMode,
}

impl<const DATA_RX_PIPE_SIZE: usize, const RX_BUF_SIZE: usize>
    RxPump<'_, DATA_RX_PIPE_SIZE, RX_BUF_SIZE>
{
    pub async fn run<R>(self, mut uart_rx: R) -> !
    where
        R: Read,
    {
        let mut data_rx_writer = self.data_rx_writer;
        let mut ingress = self.ingress;

        let mut receiver = self.mode.watch.receiver().unwrap();

        loop {
            if self.mode.is_data_mode() {
                let mut buf = [0u8; RX_BUF_SIZE];
                match select(uart_rx.read(&mut buf), receiver.changed()).await {
                    Either::First(Ok(received)) if received > 0 => {
                        #[cfg(feature = "defmt")]
                        defmt::trace!("[Data][RX] received {} bytes", received);
                        data_rx_writer.write_all(&buf[..received]).await.unwrap();
                        #[cfg(feature = "defmt")]
                        defmt::trace!("[Data][RX] enqueued {} bytes", received);
                    }
                    _ => continue,
                }
            } else {
                let ingress_buf = ingress.write_buf();
                match select(uart_rx.read(ingress_buf), receiver.changed()).await {
                    Either::First(Ok(received)) if received > 0 => {
                        #[cfg(feature = "defmt")]
                        defmt::trace!("[AT][RX] received {} bytes", received);
                        ingress.try_advance(received).unwrap();
                        #[cfg(feature = "defmt")]
                        defmt::trace!("[AT][RX] enqueued {} bytes", received);
                    }
                    _ => continue,
                };
            }
        }
    }
}

pub struct PppIo<'a, const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize> {
    tx: Writer<'a, CriticalSectionRawMutex, DATA_TX_PIPE_SIZE>,
    rx: Reader<'a, CriticalSectionRawMutex, DATA_RX_PIPE_SIZE>,
}

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug)]
pub struct PppIoError;

impl core::error::Error for PppIoError {}

impl embedded_io_async::Error for PppIoError {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        embedded_io_async::ErrorKind::Other
    }
}

impl fmt::Display for PppIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ppp io error")
    }
}

impl<'a, const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize>
    PppIo<'a, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>
{
    fn new(
        tx: Writer<'a, CriticalSectionRawMutex, DATA_TX_PIPE_SIZE>,
        rx: Reader<'a, CriticalSectionRawMutex, DATA_RX_PIPE_SIZE>,
    ) -> Self {
        Self { tx, rx }
    }
}

impl<const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize> embedded_io_async::ErrorType
    for PppIo<'_, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>
{
    type Error = PppIoError;
}

impl<const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize> Write
    for PppIo<'_, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Ok(self.tx.write(buf).await)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.tx.flush().await.map_err(|_| PppIoError)
    }
}

impl<const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize> Read
    for PppIo<'_, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        Ok(self.rx.read(buf).await)
    }
}

impl<const DATA_TX_PIPE_SIZE: usize, const DATA_RX_PIPE_SIZE: usize> BufRead
    for PppIo<'_, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>
{
    async fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        Ok(self.rx.fill_buf().await)
    }

    fn consume(&mut self, amt: usize) {
        self.rx.consume(amt);
    }
}
