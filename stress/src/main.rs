use clap::Parser;

mod args;
mod local;
mod runner;
mod runtime;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod hybrid;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod redis;

use args::{Args, Provider};

fn main() {
    let mut args = Args::parse();

    if let Err(error) = args.validate() {
        eprintln!("invalid stress configuration: {error}");
        std::process::exit(2);
    }

    args.add_run_namespace();

    match args.provider {
        Provider::Local => local::run(&args),

        Provider::Redis => {
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            redis::run(&args);

            #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
            {
                eprintln!(
                    "redis provider requires a runtime feature: \
                     --features redis-tokio  or  --features redis-smol"
                );
                std::process::exit(2);
            }
        }

        Provider::Hybrid => {
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            hybrid::run(&args);

            #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
            {
                eprintln!(
                    "hybrid provider requires a runtime feature: \
                     --features redis-tokio  or  --features redis-smol"
                );
                std::process::exit(2);
            }
        }
    }
} // end fn main
