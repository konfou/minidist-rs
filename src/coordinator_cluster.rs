use tokio::process::Command;
use tokio::time::{Duration, sleep};

pub async fn run(workers: (u16, u16)) -> anyhow::Result<()> {
    let (start, end) = workers;

    for port in start..=end {
        println!("Starting worker on port {port}...");

        let exe_dir = std::env::current_exe()?.parent().unwrap().to_path_buf();
        let worker_bin = exe_dir.join("worker");

        Command::new(&worker_bin)
            .arg("--port")
            .arg(port.to_string())
            .spawn()?;
    }

    sleep(Duration::from_millis(300)).await;

    println!("All workers started.\n");
    Ok(())
}
