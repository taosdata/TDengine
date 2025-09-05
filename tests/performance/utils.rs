use chrono::Local;
use tokio::process::Command;

/// 获取本机首选的 IPv4 地址，用于在未显式提供 HOST 环境变量时区分测试来源机器。
/// 优先通过 UdpSocket 根据路由表获取 (不会真正发送数据)。失败时回退到 127.0.0.1。
pub fn local_ipv4() -> String {
    // 通过与公共地址建立“伪”连接来获取本地出站 IP。
    if let std::result::Result::Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if sock.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = sock.local_addr() {
                let ip = addr.ip();
                if ip.is_ipv4() {
                    return ip.to_string();
                }
            }
        }
    }
    // 兜底
    "127.0.0.1".to_string()
}

/// 探测当前系统的 max_buffer。
pub fn detect_max_buffer(max_vgroups: usize) -> usize {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_memory();
    let mut avail = sys.available_memory();
    if avail == 0 {
        let total = sys.total_memory();
        let used = sys.used_memory();
        let free_calc = total.saturating_sub(used);
        let free_mem = sys.free_memory();
        // 选择一个非零的备用值: 优先 total-used, 否则 free_memory, 最后保持 0
        let fallback = if free_calc > 0 {
            free_calc
        } else if free_mem > 0 {
            free_mem
        } else {
            0
        };
        if fallback > 0 {
            tracing::warn!(
                "available_memory reported 0; fallback applied: total={} used={} fallback={} (bytes)",
                total,
                used,
                fallback
            );
            avail = fallback;
        } else {
            tracing::warn!(
                "available_memory and fallback values are 0; defaulting to minimal buffer logic"
            );
        }
    }
    compute_max_buffer(avail, max_vgroups)
}

/// 根据可用内存字节数与最大 vgroups 计算 max_buffer。
/// 规则：
/// 1. avail_mem_mb = bytes / 1024^2 (总可用 MiB)
/// 2. 从环境变量读取比例，默认 0.5，可配置 0 < p <= 1
/// 3. portion_total_mb = floor(avail_mem_mb * percent)
/// 4. per_vgroup_portion_mb = portion_total_mb / max_vgroups (按最大 vgroups 平摊)
/// 5. max_buffer = max(floor(per_vgroup_portion_mb), 3)
pub fn compute_max_buffer(avail_mem_bytes: u64, max_vgroups: usize) -> usize {
    let avail_mem_mb = avail_mem_bytes as f64 / 1024.0 / 1024.0;
    // 从环境变量读取比例，默认 0.5，可配置 0 < p <= 1
    let percent: f64 = std::env::var("BUFFER_PERCENT")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|p| {
            if p <= 0.0 {
                0.5
            } else if p > 1.0 {
                1.0
            } else {
                p
            }
        })
        .unwrap_or(0.5);
    let portion_total_mb = (avail_mem_mb * percent).floor();
    let per_vgroup_portion_mb = if max_vgroups > 0 {
        portion_total_mb / max_vgroups as f64
    } else {
        0.0
    };
    let max_buffer = std::cmp::max(per_vgroup_portion_mb.floor() as usize, 3);

    tracing::info!(
        "avail_mem_mb={:.2}MB, percent={:.2}, portion_total_mb={:.2}MB, per_vgroup_portion_mb={:.2}MB (max_vgroups={}), max_buffer={}",
        avail_mem_mb,
        percent,
        portion_total_mb,
        per_vgroup_portion_mb,
        max_vgroups,
        max_buffer
    );

    max_buffer
}

/// 获取 TDengine 的版本信息
/// 通过执行 `taosd -V` 命令获取
pub async fn taosd_version() -> anyhow::Result<String> {
    let output = Command::new("taosd")
        .arg("-V")
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("failed to execute taosd -V: {e}"))?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "taosd -V exited with status: {}",
            output.status
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let version_text = stdout.trim();
    if version_text.is_empty() {
        return Err(anyhow::anyhow!("taosd -V produced empty stdout"));
    }

    Ok(escape_csv(version_text))
}

/// 获取 taosX 的版本信息
/// version: `git describe --tags --abbrev=0 | cut -d - -f 2`
/// git: `git rev-parse HEAD`
/// build: $OS-$ARCH $DATETIME $TIMEZONE
pub async fn taosx_version() -> anyhow::Result<String> {
    // 1. 获取 taosx 版本 (依据最近的 tag)。约定 tag 形如 <prefix>-<version>，例如 taosx-3.3.6.12
    let desc_out = Command::new("git")
        .arg("describe")
        .arg("--tags")
        .arg("--always")
        .arg("--abbrev=0")
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("failed to execute git describe: {e}"))?;
    if !desc_out.status.success() {
        return Err(anyhow::anyhow!(
            "git describe exited with status: {}",
            desc_out.status
        ));
    }
    let tag_raw = String::from_utf8_lossy(&desc_out.stdout);
    let tag = tag_raw.trim();
    // 提取版本号：按 '-' 切分，取第二段；如果不存在则使用整个 tag
    let version = tag
        .split('-')
        .nth(1)
        .map(|s| s.to_string())
        .unwrap_or_else(|| tag.to_string());

    // 2. 获取当前提交 hash
    let rev_out = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("failed to execute git rev-parse: {e}"))?;
    if !rev_out.status.success() {
        return Err(anyhow::anyhow!(
            "git rev-parse exited with status: {}",
            rev_out.status
        ));
    }
    let commit = String::from_utf8_lossy(&rev_out.stdout).trim().to_string();

    // 3. 构造 build 信息 (当前构建时间 + 平台)
    let build_time = Local::now().format("%Y-%m-%d %H:%M:%S %:z").to_string();
    let platform = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);

    let taosx_ver =
        format!("taosx version: {version}\ngit: {commit}\nbuild: {platform} {build_time}");

    Ok(escape_csv(&taosx_ver))
}

fn escape_csv(value: &str) -> String {
    // 替换逗号和换行符，并折叠空白
    value
        .replace(',', ";")
        .replace(['\r', '\n'], ";")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_ipv4() {
        let ip = local_ipv4();
        println!("Local IPv4: {}", ip);
        assert!(!ip.is_empty());
        assert!(ip.parse::<std::net::Ipv4Addr>().is_ok());
    }

    #[tokio::test]
    async fn test_taosd_version() {
        let output = Command::new("taosd").arg("-V").output().await;
        if let Ok(out) = output {
            if out.status.success() {
                let version = taosd_version().await.unwrap();
                assert!(!version.is_empty(), "TDengine version should not be empty");
                println!("{}", version);
            }
        }
    }

    #[tokio::test]
    async fn test_taosx_version() {
        let version = taosx_version().await.unwrap();
        assert!(!version.is_empty(), "taosx version should not be empty");
        println!("{}", version);
    }

    #[test]
    fn test_detect_max_buffer() {
        let max_buffer = detect_max_buffer(64);
        assert!(max_buffer >= 3);
    }

    #[test]
    fn test_compute_max_buffer() {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .init();

        let max_vgroups = 64usize;

        let mbuf = compute_max_buffer(5 * 1024 * 1024, max_vgroups); // 5MB
        assert_eq!(mbuf, 3); // min 3

        let mbuf = compute_max_buffer(100 * 1024 * 1024, max_vgroups);
        assert_eq!(mbuf, 3);

        let eight_gb: u64 = 8 * 1024 * 1024 * 1024;
        let mbuf = compute_max_buffer(eight_gb, max_vgroups);
        assert_eq!(mbuf, 64);
    }
}
