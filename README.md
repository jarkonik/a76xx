# a76xx

Pure Rust `embedded-io-async` and `embedded-hal` based driver for SIMCom `a76xx` family of modems.

### Example

Full example for `esp32` platform lives in the repository's `examples/esp32`
directory. The most important parts, with platform specific initialization
ommited, follow below.

```rust
// Those may need to be adjusted for best bandwidth
const DATA_TX_PIPE_SIZE: usize = 1024;
const DATA_RX_PIPE_SIZE: usize = 1024;
const TX_BUF_SIZE: usize = 256;
const RX_BUF_SIZE: usize = 256;
const TCP_TX_BUF_SIZE: usize = 4096;
const TCP_RX_BUF_SIZE: usize = 4096;

(...)

// Allocation-elastic modem internal resources. For best results may need
// to be heap allocated.
static MODEM_RESOURCES: StaticCell<
    a76xx::ModemResources<DATA_TX_PIPE_SIZE, DATA_RX_PIPE_SIZE, TX_BUF_SIZE, RX_BUF_SIZE>,
> = StaticCell::new();
let modem_resources = MODEM_RESOURCES.init_with(a76xx::ModemResources::new);

// Main modem object. PowerPins structure configuration will depend on your wiring.
// Pin fields need to implement relevant `embedded-hal` digital output pin traits.
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

(...)

// Spawn pump tasks for rx and tx, example assumes `embassy` framework, although
// this can be done by any async executor.
spawner.spawn(rx_pump_task(uart_reader, rx_pump).unwrap());
spawner.spawn(tx_pump_task(tx_pump, uart_writer).unwrap());

// Wait for connection i.e. successful OK response to an AT command.
modem.wait_for_connection().await;

// Pin for your SIM cards, env provided here.
let pin: &str = env!("PIN");

// Establish PPP data connection.
// `ppp_data_pipe` is a bidirectional PPP data stream.
let ppp_data_pipe = unwrap!(modem.connect_ppp(pin, "internet", "*99#").await);
(...)

#[embassy_executor::task]
async fn tx_pump_task(
    tx_pump: a76xx::TxPump<'static, DATA_TX_PIPE_SIZE, TX_BUF_SIZE>,
    uart_tx: UartTx<'static, Async>,
) -> ! {
    tx_pump.run(uart_tx).await
}

#[embassy_executor::task]
async fn rx_pump_task(
    uart_rx: UartRx<'static, Async>,
    rx_pump: a76xx::RxPump<'static, DATA_RX_PIPE_SIZE, RX_BUF_SIZE>,
) -> ! {
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
```

### Supported modems

- [x] A7670X(A7670 variants: A7670E, A7670G etc.)

### License

a76xx is licensed under either of the following, at your option: Apache License, Version 2.0, (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0) or MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)

### Special thanks

- to Karol Więcław for purchasing a A7670E-based board which has been used for initial development
