use std::path::Path;
use std::process::{Child, Command};
use tokio::time::{Duration, sleep};

pub fn count_segments(table_dir: &Path) -> anyhow::Result<usize> {
    let mut count = 0usize;
    let entries = std::fs::read_dir(table_dir)
        .map_err(|e| anyhow::anyhow!("Failed to read table dir {:?}: {}", table_dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| anyhow::anyhow!("Failed to read entry: {}", e))?;
        let file_type = entry
            .file_type()
            .map_err(|e| anyhow::anyhow!("Failed to read entry type: {}", e))?;
        if file_type.is_dir() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("seg-") {
                count += 1;
            }
        }
    }

    if count == 0 {
        return Err(anyhow::anyhow!(
            "No segment directories (seg-*) found under table dir"
        ));
    }

    Ok(count)
}

pub fn resolve_worker_ports(spec: &[u16], segments: usize) -> anyhow::Result<Vec<u16>> {
    if spec.len() == 1 {
        let start = spec[0];
        let mut ports = Vec::with_capacity(segments);
        for offset in 0..segments {
            let port = start.checked_add(offset as u16).ok_or_else(|| {
                anyhow::anyhow!("Worker port overflow when deriving from start value")
            })?;
            ports.push(port);
        }
        Ok(ports)
    } else if spec.len() == segments {
        Ok(spec.to_vec())
    } else {
        Err(anyhow::anyhow!(
            "Expected either one worker port (start) or {} ports, got {}",
            segments,
            spec.len()
        ))
    }
}

pub struct WorkerCluster {
    children: Vec<Child>,
}

impl WorkerCluster {
    pub async fn spawn(worker_ports: &[u16], table: &Path) -> anyhow::Result<Self> {
        let mut children = Vec::new();
        for (i, port) in worker_ports.iter().enumerate() {
            println!("Starting worker on port {port} (segment {i})...");

            let exe_dir = std::env::current_exe()?.parent().unwrap().to_path_buf();
            let worker_bin = exe_dir.join("worker");

            let child = Command::new(&worker_bin)
                .arg("--port")
                .arg(port.to_string())
                .arg("--table")
                .arg(table.display().to_string())
                .arg("--segment")
                .arg(i.to_string())
                .spawn()?;

            children.push(child);
        }

        sleep(Duration::from_millis(300)).await;

        println!("All workers started.\n");
        Ok(WorkerCluster { children })
    }
}

impl Drop for WorkerCluster {
    fn drop(&mut self) {
        for child in self.children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
