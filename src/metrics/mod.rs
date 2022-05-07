use metrics::{counter, describe_gauge, gauge, histogram, register_gauge, register_histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::{
    collections::HashSet,
    net::SocketAddr,
    time::{Duration, Instant},
};

#[derive(Debug, Default)]
pub struct Metrics {
    listen: Option<SocketAddr>,
    push: Option<String>,
    push_interval: Option<Duration>,
    interval: Option<u16>,
}

pub fn process_metrics_init() {
    register_gauge!("taosx_process_cpu_percent");

    describe_gauge!("taosx_process_cpu_percent", "CPU percent of the process");
}

pub fn process_metrics() -> anyhow::Result<()> {
    let ps = procfs::process::Process::myself()?;
    let stat = &ps.stat;
    let ticks = procfs::ticks_per_second()?;
    let mut proc = psutil::process::Process::current()?;

    let cpu = proc.cpu_percent()?;
    gauge!("taosx_process_cpu_percent", cpu as f64);

    let mem = proc.memory_percent()?;
    gauge!(
        "taosx_process_mem_percent",
        (mem * 10000.0) as u64 as f64 / 100.0
    );

    let threads = ps.tasks()?.count();
    gauge!("taosx_process_threads", threads as f64);

    let open_files = proc.open_files()?.len();
    gauge!("taosx_process_open_files", open_files as f64);

    let uptime = (ps.stat.utime + ps.stat.stime) as f64 / ticks as f64;
    gauge!("taosx_process_uptime", uptime as f64);

    let io = ps.io()?;
    gauge!("taosx_process_io_read_bytes", io.read_bytes as f64);
    gauge!("taosx_process_io_write_bytes", io.write_bytes as f64);

    let fd = ps.fd()?;
    let mut inodes = HashSet::new();
    for fd in fd {
        use procfs::process::FDTarget;
        match fd.target {
            FDTarget::Net(inode) => {
                inodes.insert(inode);
            }
            FDTarget::Socket(inode) => {
                inodes.insert(inode);
            }
            _ => {}
        }
    }

    let (mut rx, mut tx) = (0, 0);

    let tcp = procfs::net::tcp().unwrap();
    let tcp6 = procfs::net::tcp6().unwrap();
    for entry in tcp.into_iter().chain(tcp6) {
        // find the process (if any) that has an open FD to this entry's inode
        let local_address = format!("{}", entry.local_address);
        let remote_addr = format!("{}", entry.remote_address);
        let state = format!("{:?}", entry.state);
        if inodes.contains(&entry.inode) {
            log::debug!(
                "{:<26} {:<26} {:<15} {:<12} {}/{} {}/{}",
                local_address,
                remote_addr,
                state,
                entry.inode,
                stat.pid,
                stat.comm,
                entry.rx_queue,
                entry.tx_queue
            );
            rx += entry.rx_queue;
            tx += entry.tx_queue;
        }
    }

    gauge!("taosx_process_net_rx", rx as f64);
    gauge!("taosx_process_net_tx", tx as f64);
    Ok(())
}

impl Metrics {
    pub fn init(self) -> anyhow::Result<()> {
        let mut exporter = PrometheusBuilder::new();
        let interval = self.interval();
        let dur = Duration::from_secs(interval as u64);

        if let Some(listen) = self.listen {
            exporter = exporter.with_http_listener(listen);
        }
        if let Some(push) = self.push {
            let interval = self.push_interval.unwrap_or(Duration::from_secs(30));
            exporter = exporter.with_push_gateway(push, interval)?;
        }
        let _ = exporter.install()?;
        process_metrics_init();
        std::thread::spawn(move || loop {
            let _ = process_metrics();
            metrics::increment_counter!("up seconds");
            std::thread::sleep(dur);
        });
        Ok(())
    }

    pub fn interval(&self) -> u16 {
        self.interval.unwrap_or(1)
    }
}
