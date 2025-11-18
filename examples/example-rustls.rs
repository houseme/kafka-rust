fn main() {
    example::main();
}

#[cfg(feature = "security-rustls")]
mod example {
    use kafka;
    use tracing::info;

    use std::env;
    use std::process;

    use self::kafka::client::{FetchOffset, KafkaClient, SecurityConfig};

    pub fn main() {
        tracing_subscriber::fmt::init();

        // ~ parse the command line arguments
        let cfg = match Config::from_cmdline() {
            Ok(cfg) => cfg,
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
        };

        // ~ Create SecurityConfig with rustls
        let mut security_config = SecurityConfig::new();

        // Set hostname verification (default is true)
        security_config = security_config.with_hostname_verification(cfg.verify_hostname);

        // Load CA certificate if provided
        if let Some(ca_cert) = cfg.ca_cert {
            info!("loading ca-file={}", ca_cert);
            security_config = security_config.with_ca_cert(ca_cert);
        }

        // Load client certificate and key if provided
        if let (Some(client_cert), Some(client_key)) = (cfg.client_cert, cfg.client_key) {
            info!("loading cert-file={}, key-file={}", client_cert, client_key);
            security_config = security_config.with_client_cert(client_cert, client_key);
        }

        // ~ instantiate KafkaClient with rustls TLS support
        let mut client = KafkaClient::new_secure(cfg.brokers, security_config);

        // ~ communicate with the brokers
        match client.load_metadata_all() {
            Err(e) => {
                println!("{:?}", e);
                drop(client);
                process::exit(1);
            }
            Ok(_) => {
                // ~ at this point we have successfully loaded
                // metadata via a secured connection to one of the
                // specified brokers

                if client.topics().is_empty() {
                    println!("No topics available!");
                } else {
                    // ~ now let's communicate with all the brokers in
                    // the cluster our topics are spread over

                    let topics: Vec<String> = client.topics().names().map(Into::into).collect();
                    match client.fetch_offsets(topics.as_slice(), FetchOffset::Latest) {
                        Err(e) => {
                            println!("{:?}", e);
                            drop(client);
                            process::exit(1);
                        }
                        Ok(toffsets) => {
                            println!("Topic offsets:");
                            for (topic, mut offs) in toffsets {
                                offs.sort_by_key(|x| x.partition);
                                println!("{}", topic);
                                for off in offs {
                                    println!("\t{}: {:?}", off.partition, off.offset);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    struct Config {
        brokers: Vec<String>,
        client_cert: Option<String>,
        client_key: Option<String>,
        ca_cert: Option<String>,
        verify_hostname: bool,
    }

    impl Config {
        fn from_cmdline() -> Result<Config, String> {
            let mut opts = getopts::Options::new();
            opts.optflag("h", "help", "Print this help screen");
            opts.optopt(
                "",
                "brokers",
                "Specify kafka brokers (comma separated)",
                "HOSTS",
            );
            opts.optopt("", "ca-cert", "Specify the trusted CA certificates", "FILE");
            opts.optopt("", "client-cert", "Specify the client certificate", "FILE");
            opts.optopt(
                "",
                "client-key",
                "Specify key for the client certificate",
                "FILE",
            );
            opts.optflag(
                "",
                "no-hostname-verification",
                "Do not perform server hostname verification (insecure!)",
            );

            let args: Vec<_> = env::args().collect();
            let m = match opts.parse(&args[1..]) {
                Ok(m) => m,
                Err(e) => return Err(format!("{}", e)),
            };

            if m.opt_present("help") {
                let brief = format!("{} [options]", args[0]);
                return Err(opts.usage(&brief));
            };

            let brokers = m
                .opt_str("brokers")
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_else(|| vec!["localhost:9093".into()]);
            if brokers.is_empty() {
                return Err("Invalid --brokers specified!".to_owned());
            }

            Ok(Config {
                brokers,
                client_cert: m.opt_str("client-cert"),
                client_key: m.opt_str("client-key"),
                ca_cert: m.opt_str("ca-cert"),
                verify_hostname: !m.opt_present("no-hostname-verification"),
            })
        }
    }
}

#[cfg(not(feature = "security-rustls"))]
mod example {
    use std::process;

    pub fn main() {
        println!("example relevant only with the \"security-rustls\" feature enabled!");
        println!("Try: cargo run --example example-rustls --features=security-rustls");
        process::exit(1);
    }
}
