#![no_std]
#![no_main]
#![deny(
    clippy::mem_forget,
    reason = "mem::forget is generally not safe to do with esp_hal types, especially those \
    holding buffers for the duration of a data transfer."
)]

use embassy_net::dns::DnsSocket;
use embassy_net::tcp::client::{TcpClient, TcpClientState};
use embassy_net::{ConfigV4, Ipv4Address, Ipv4Cidr, Stack};
use embassy_net_ppp::Runner;
use embedded_io_async::Read as _;
use reqwless::client::HttpClient;

use defmt::unwrap;
use defmt::{error, info, warn};
use embassy_executor::Spawner;
use embassy_time::{Duration, Instant, Timer};
use esp_hal::gpio::{Level, Output, OutputConfig};
use esp_hal::timer::timg::TimerGroup;
use esp_hal::uart::Uart;
use esp_hal::uart::{UartRx, UartTx};
use esp_hal::{Async, uart};
use heapless::Vec;
use static_cell::StaticCell;
use {esp_backtrace as _, esp_println as _};

// This creates a default app-descriptor required by the esp-idf bootloader.
// For more information see: <https://docs.espressif.com/projects/esp-idf/en/stable/esp32/api-reference/system/app_image_format.html#application-description>
esp_bootloader_esp_idf::esp_app_desc!();

// Those may need to be adjusted for best bandwidth
const DATA_TX_PIPE_SIZE: usize = 1024;
const DATA_RX_PIPE_SIZE: usize = 1024;
const TX_BUF_SIZE: usize = 256;
const RX_BUF_SIZE: usize = 256;
const TCP_TX_BUF_SIZE: usize = 4096;
const TCP_RX_BUF_SIZE: usize = 4096;

#[embassy_executor::task]
async fn tx_pump_task(
    tx_pump: a76xx::TxPump<'static, DATA_TX_PIPE_SIZE, TX_BUF_SIZE>,
    uart_tx: UartTx<'static, Async>,
) -> ! {
    info!("Spawned tx pump task");
    tx_pump.run(uart_tx).await
}

#[embassy_executor::task]
async fn rx_pump_task(
    uart_rx: UartRx<'static, Async>,
    rx_pump: a76xx::RxPump<'static, DATA_RX_PIPE_SIZE, RX_BUF_SIZE>,
) -> ! {
    info!("Spawned rx pump task");
    rx_pump.run(uart_rx).await
}

#[embassy_executor::task]
async fn net_task(mut runner: embassy_net::Runner<'static, embassy_net_ppp::Device<'static>>) -> ! {
    runner.run().await
}

#[embassy_executor::task]
async fn ppp_task(
    stack: Stack<'static>,
    data_pipe: a76xx::PppIo<'static, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>,
    mut runner: Runner<'static>,
) -> ! {
    info!("Spawned ppp task");

    let cfg = embassy_net_ppp::Config {
        username: "internet".as_bytes(),
        password: "internet".as_bytes(),
    };

    match unwrap!(
        runner
            .run(data_pipe, cfg, |ipv4| {
                let Some(addr) = ipv4.address else {
                    warn!("PPP did not provide an IP address.");
                    return;
                };
                let mut dns_servers: heapless::Vec<Ipv4Address, 3> = Vec::new();
                for s in ipv4.dns_servers.iter().flatten() {
                    let _ = dns_servers.push(*s);
                }
                let config = ConfigV4::Static(embassy_net::StaticConfigV4 {
                    address: Ipv4Cidr::new(addr, 0),
                    gateway: None,
                    dns_servers,
                });
                stack.set_config_v4(config);
            })
            .await
    ) {}
}

#[esp_rtos::main]
async fn main(spawner: Spawner) -> ! {
    let config = esp_hal::Config::default();
    let peripherals = esp_hal::init(config);

    let timg0 = TimerGroup::new(peripherals.TIMG0);
    let sw_ints =
        esp_hal::interrupt::software::SoftwareInterruptControl::new(peripherals.SW_INTERRUPT);
    esp_rtos::start(timg0.timer0, sw_ints.software_interrupt0);

    // Allocation-elastic modem internal resources. For best results may need
    // to be heap allocated.
    static MODEM_RESOURCES: StaticCell<
        a76xx::ModemResources<DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE, TX_BUF_SIZE, RX_BUF_SIZE>,
    > = StaticCell::new();
    let modem_resources = MODEM_RESOURCES.init_with(a76xx::ModemResources::new);

    // Main modem object. PowerPins structure configuration will depend on your wiring.
    let (mut modem, rx_pump, tx_pump) = a76xx::Modem::new(
        modem_resources,
        a76xx::PowerPins {
            power_control: Output::new(peripherals.GPIO12, Level::Low, OutputConfig::default()),
            reset: Output::new(peripherals.GPIO5, Level::Low, OutputConfig::default()),
            dtr: Output::new(peripherals.GPIO25, Level::Low, OutputConfig::default()),
            power_on: Output::new(peripherals.GPIO4, Level::Low, OutputConfig::default()),
        },
    );
    // Performs power on sequence using PowerPins defined outputs.
    modem.power_on().await.unwrap();

    // Fifo size and baudrate may need to be adjusted for best bandwidth.
    // It is also possible to switch bandwidth after establishing wait_for_connection
    // using `modem.request_baudrate(rate).await` API. Default baudrate for the modem I tested
    // is 115200 so the initial connection had to be established using 115200.
    let uart_rx_config = uart::RxConfig::default().with_fifo_full_threshold(1);
    let uart = Uart::new(
        peripherals.UART1,
        uart::Config::default()
            .with_baudrate(115_200)
            .with_parity(uart::Parity::None)
            .with_stop_bits(uart::StopBits::_1)
            .with_data_bits(uart::DataBits::_8)
            .with_rx(uart_rx_config),
    )
    .unwrap()
    .with_rx(peripherals.GPIO27)
    .with_tx(peripherals.GPIO26)
    .into_async();
    let (uart_reader, uart_writer) = uart.split();

    // Spawn pumps for rx and tx
    spawner.spawn(rx_pump_task(uart_reader, rx_pump).unwrap());
    spawner.spawn(tx_pump_task(tx_pump, uart_writer).unwrap());

    // Wait for connection i.e. successful OK response to an AT command.
    modem.wait_for_connection().await;

    // Pin for your SIM cards, env provided here.
    let pin: &str = env!("PIN");

    // Establish PPP data connection.
    // `ppp_data_pipe` is a bidirection PPP data stream.
    let ppp_data_pipe = unwrap!(modem.connect_ppp(pin, "internet", "*99#").await);

    // Spawn an example HTTP application that uses the data pipe.
    spawner.spawn(unwrap!(app_task(spawner, ppp_data_pipe)));

    // Idle loop
    loop {
        Timer::after(Duration::from_millis(1000)).await;
    }
}

#[embassy_executor::task]
async fn app_task(
    spawner: Spawner,
    data_pipe: a76xx::PppIo<'static, DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE>,
) -> ! {
    let rng = esp_hal::rng::Rng::new();
    let mut seed = [0; 8];
    rng.read(&mut seed);
    let seed = u64::from_le_bytes(seed);

    static STATE: StaticCell<embassy_net_ppp::State<4, 4>> = StaticCell::new();
    let state = STATE.init_with(embassy_net_ppp::State::<4, 4>::new);
    let (device, runner) = embassy_net_ppp::new(state);

    static RESOURCES: StaticCell<embassy_net::StackResources<4>> = StaticCell::new();
    let (stack, net_runner) = embassy_net::new(
        device,
        embassy_net::Config::default(),
        RESOURCES.init_with(embassy_net::StackResources::new),
        seed,
    );

    spawner.spawn(unwrap!(net_task(net_runner)));
    spawner.spawn(unwrap!(ppp_task(stack, data_pipe, runner)));
    stack.wait_link_up().await;

    let dns = DnsSocket::new(stack);
    static TCP_STATE: StaticCell<TcpClientState<1, TCP_TX_BUF_SIZE, TCP_RX_BUF_SIZE>> =
        StaticCell::new();
    let tcp_state = TCP_STATE.init_with(TcpClientState::<1, TCP_TX_BUF_SIZE, TCP_RX_BUF_SIZE>::new);
    let mut tcp = TcpClient::new(stack, tcp_state);
    tcp.set_timeout(Some(Duration::from_secs(30)));

    let mut client = HttpClient::new(&tcp, &dns);
    let mut i = 0;
    loop {
        info!("Request {}", i);
        call(&mut client).await;
        Timer::after(Duration::from_millis(100)).await;
        i += 1;
    }
}

async fn call(
    client: &mut HttpClient<'_, TcpClient<'_, 1, TCP_TX_BUF_SIZE, TCP_RX_BUF_SIZE>, DnsSocket<'_>>,
) {
    let mut buffer = [0u8; 4096];
    let mut buffer2 = [0u8; 4096];
    let mut http_req = match client
        .request(
            reqwless::request::Method::GET,
            "http://speed.cloudflare.com/__down?bytes=1048576",
        )
        .await
    {
        Ok(ok) => ok,
        Err(e) => {
            error!("HTTP request error: {:?}", e);
            return;
        }
    };
    let before_send = Instant::now();
    let response = http_req.send(&mut buffer).await;
    match response {
        Ok(response) => {
            let now = Instant::now();
            info!(
                "Got response {} in {}ms",
                response.status,
                (now - before_send).as_millis()
            );

            let mut interval_bytes = 0usize;

            let mut last_report = Instant::now();

            let mut total = 0;

            let content_length = response.content_length;

            let mut reader = response.body().reader();

            loop {
                let n = match reader.read(&mut buffer2[..]).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("HTTP body read error: {:?}", e);
                        return;
                    }
                };
                if n == 0 {
                    break;
                }

                total += n;
                interval_bytes += n;

                let now = Instant::now();
                if now - last_report >= Duration::from_secs(1) {
                    let elapsed = now - last_report;

                    let kb_per_s = interval_bytes as u64 * 1000 / elapsed.as_millis() / 1024;

                    if let Some(content_length) = content_length {
                        defmt::info!("{}/{}({} kB/s)", total, content_length, kb_per_s);
                    } else {
                        defmt::info!("{}({} kB/s)", total, kb_per_s);
                    }

                    interval_bytes = 0;
                    last_report = now;
                }
            }
        }
        Err(e) => {
            error!("HTTP response error: {:?}", e);
        }
    }
}
