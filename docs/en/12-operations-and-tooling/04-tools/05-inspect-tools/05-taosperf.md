---
sidebar_label: Performance Benchmark Tool
title: Performance Benchmark Tool
toc_max_heading_level: 4
---

`taosperf` uses tools such as `dd`, `fio`, `ping`, and `iperf3` to evaluate the disk and network performance of a TDengine deployment environment.

## Usage

Local mode tests the current host and can test network throughput to a specified peer:

```text
usage: taosperf local [-h] [--config CONFIG] [--backend]
                      [--test-item {all,disk,network}] [--result RESULT]
                      [--log-level {debug,info}] [--ip IP]
```

SSH mode runs the configured tests across all cluster nodes:

```text
usage: taosperf ssh [-h] [--config CONFIG] [--backend]
                    [--test-item {all,disk,network}] [--result RESULT]
                    [--log-level {debug,info}]
```

`--test-item` selects all tests, disk tests only, or network tests only. `--result` sets the report directory, and `--ip` sets the network-test peer in local mode.

## Configuration

The YAML configuration defines cluster hosts and disk paths, plus the commands used for disk and network testing:

```yaml
nodes:
  - ip: 192.168.1.101
    user: root
    password: yourpassword
    port: 22
    dirs:
      - dir: /data1
      - dir: /data2

cmd:
  - io:
      dd:
        write: dd if=/dev/zero of=[file] bs=400K count=50000 oflag=direct conv=fsync
        read: dd if=[file] of=/dev/null bs=1M count=20000 iflag=direct
  - network:
      ping: ping -c 10000 -i 0.1 -s 1024 [ip]
      iperf3: iperf3 -c [ip] -P 4 -t 120
```

## Output

The tool writes `perf_report.md`, which records disk and network configuration, the commands executed, and measured disk read/write and network throughput.

## Examples

```bash
# Test local disks and network connectivity
./taosperf local

# Test the current host against a specific peer
./taosperf local -i 192.168.1.1

# Test all configured cluster nodes
./taosperf ssh -f /path/to/perf.yaml

# Run only network or disk tests
./taosperf ssh -ti network
./taosperf ssh -ti disk
```
