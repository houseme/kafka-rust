fn main() {
    example::main();
}

#[cfg(any(feature = "security", feature = "security-ring"))]
mod example {
    use std::{env, process};

    use rustfs_kafka::client::{FetchOffset, KafkaClient, SecurityConfig};

    pub fn main() {
        tracing_subscriber::fmt::init();

        let cfg = match Config::from_cmdline() {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("{e}");
                process::exit(1);
            }
        };

        if let Err(e) = run(cfg) {
            eprintln!("rustls example failed: {e}");
            process::exit(1);
        }
    }

    fn run(cfg: Config) -> rustfs_kafka::Result<()> {
        let mut security = SecurityConfig::new().with_hostname_verification(cfg.verify_hostname);

        if let Some(ca_cert) = cfg.ca_cert {
            security = security.with_ca_cert(ca_cert);
        }
        if let (Some(client_cert), Some(client_key)) = (cfg.client_cert, cfg.client_key) {
            security = security.with_client_cert(client_cert, client_key);
        }

        let mut client = KafkaClient::new_secure(cfg.brokers, security);
        client.load_metadata_all()?;

        let topics: Vec<String> = client.topics().names().map(ToOwned::to_owned).collect();
        if topics.is_empty() {
            println!("No topics available");
            return Ok(());
        }

        let latest = client.fetch_offsets(&topics, FetchOffset::Latest)?;
        for (topic, mut offsets) in latest {
            offsets.sort_by_key(|x| x.partition);
            println!("{topic}");
            for off in offsets {
                println!("  partition={} latest={}", off.partition, off.offset);
            }
        }
        Ok(())
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
            opts.optopt("", "ca-cert", "Specify trusted CA certificates", "FILE");
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
                "Disable server hostname verification (insecure)",
            );

            let args: Vec<_> = env::args().collect();
            let m = opts.parse(&args[1..]).map_err(|e| e.to_string())?;

            if m.opt_present("help") {
                let brief = format!("{} [options]", args[0]);
                return Err(opts.usage(&brief));
            }

            let brokers: Vec<String> = m
                .opt_str("brokers")
                .map(|s| {
                    s.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_else(|| vec!["localhost:9093".to_owned()]);

            if brokers.is_empty() {
                return Err("Invalid --brokers specified".to_owned());
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

#[cfg(not(any(feature = "security", feature = "security-ring")))]
mod example {
    pub fn main() {
        eprintln!(
            "example-rustls requires a TLS feature. Try: cargo run --example example-rustls --features security"
        );
        std::process::exit(1);
    }
}
