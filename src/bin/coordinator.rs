use clap::Parser;

#[derive(Parser)]
#[command(name = "coordinator")]
#[command(about = "minidist-rs coordinator", long_about = None)]
struct Cli {
    #[arg(long)]
    port: u16,

    #[arg(long, value_parser = parse_workers)]
    workers: (u32, u32),
}

fn parse_workers(s: &str) -> Result<(u32, u32), String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        return Err("Expected format <min>,<max>".into());
    }
    let min: u32 = parts[0].parse().map_err(|_| "Invalid min worker value")?;
    let max: u32 = parts[1].parse().map_err(|_| "Invalid max worker value")?;

    if min > max {
        return Err("min must be <= max".into());
    }

    Ok((min, max))
}

fn main() {
    let cli = Cli::parse();

    println!("COORDINATOR CONFIG:");
    println!("  port = {}", cli.port);
    println!("  workers.min = {}", cli.workers.0);
    println!("  workers.max = {}", cli.workers.1);
}
