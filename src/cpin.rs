use atat::{AtatCmd, AtatResp, InternalError};
use core::str::FromStr;

#[cfg(feature = "log")]
use log::info;
use crate::Error;

// #[derive(Clone, AtatCmd)]
// #[at_cmd("+CPIN?", CpinQueryResponse, timeout_ms = 1000)]
pub struct CPINQuery;

// const AT_CMD: &'static str = "+CPIN?";


impl AtatCmd for CPINQuery {
    type Response = CpinQueryResponse;
    const MAX_LEN: usize = 8;

    fn write(&self,mut  buf: &mut [u8]) -> usize {
        let cmd = b"AT+CPIN?\r";
        let len= cmd.len();

        buf[..len].copy_from_slice(cmd);
        len
    }
    fn parse(&self, resp: Result<&[u8], InternalError>) -> Result<Self::Response, atat::Error> {
        Ok(
            CpinQueryResponse{
                code: core::str::from_utf8(resp?).expect("Failed to convert response to string").parse().map_err(|_| atat::Error::Parse)?
            }
        )
    }
}

pub struct CpinQueryResponse
{
    pub code: CpinCode,
}

#[derive(Debug)]
pub enum CpinCode{
    Ready,
    SimPin,
    SimPuk,
    PhSimPn,
    SimPin2,
    SimPuk2,
    PhNetPin,
    SimCrash,
}

impl AtatResp for CpinQueryResponse {}

impl FromStr for CpinCode{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "log")]
        info!("Parsing CPIN code: {}", s);
        match s{
            "READY" => Ok(CpinCode::Ready),
            "SIM PIN" => Ok(CpinCode::SimPin),
            "SIM PUK" => Ok(CpinCode::SimPuk),
            "PH-SIM PIN" => Ok(CpinCode::PhSimPn),
            "SIM PIN2" => Ok(CpinCode::SimPin2),
            "SIM PUK2" => Ok(CpinCode::SimPuk2),
            "PH-NET PIN" => Ok(CpinCode::PhNetPin),
            "SIM CRASH" => Ok(CpinCode::SimCrash),
            _ => Err(Error::AtCommandError { cmd: "+CPIN", source: atat::Error::Parse })
        }
    }
}