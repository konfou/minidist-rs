use clap::Parser;
use std::io::{self, Read, Write};
use std::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "repl")]
#[command(about = "minidist-rs network REPL")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long)]
    port: u16,

    #[arg(long, default_value = "/query")]
    path: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let endpoint = format!("{}:{}{}", args.host, args.port, args.path);
    println!("Connecting to coordinator at http://{} ...", endpoint);

    // PING handshake
    let ping_resp = send_request(&args, "PING")?;
    if ping_resp.trim() != "PONG" {
        eprintln!(
            "Coordinator handshake failed: expected PONG, got {}",
            ping_resp.trim()
        );
        return Ok(());
    }
    println!("Coordinator replied: {}", ping_resp.trim());

    let mut query_buf = String::new();
    let mut prompt = "minidist> ";

    loop {
        print!("{}", prompt);
        io::stdout().flush().ok();

        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }

        query_buf.push_str(&line);

        if !query_buf.trim_end().ends_with(';') {
            prompt = "... ";
            continue;
        }

        match send_request(&args, &query_buf) {
            Ok(resp) => println!("{}", resp),
            Err(e) => eprintln!("Error sending query: {}", e),
        }

        query_buf.clear();
        prompt = "minidist> ";
    }

    Ok(())
}

fn send_request(args: &Args, body: &str) -> anyhow::Result<String> {
    let addr = format!("{}:{}", args.host, args.port);
    let mut stream = TcpStream::connect(&addr)?;

    let content_len = body.as_bytes().len();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        args.path, args.host, content_len, body
    );

    stream.write_all(request.as_bytes())?;
    let mut resp = String::new();
    stream.read_to_string(&mut resp)?;

    // Extract body after headers
    if let Some(idx) = resp.find("\r\n\r\n") {
        Ok(resp[idx + 4..].to_string())
    } else {
        Err(anyhow::anyhow!("Malformed HTTP response"))
    }
}
