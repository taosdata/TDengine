# TDengine Coverage Alarm Docker Deployment Guide

## Quick Start

```bash
# Build and start the container
docker-compose up -d

# View container status
docker-compose ps

# View cron job logs
docker-compose exec tdengine-coverage-cron cat /var/log/cron/cron.log

# Follow cron job logs in real-time
docker-compose exec tdengine-coverage-cron tail -f /var/log/cron/cron.log

# Stop the container
docker-compose down
```

## Configuration

### Cron Job Schedule

- **10:10** - Run test report
- **10:12** - Run test report with cleanup
- **10:50** - Execute cleanup

### Image Selection

Using `python:3-slim` image, advantages:

- Based on Debian, good compatibility
- Small size (approximately 45MB compressed)
- Includes complete Python standard library

### Log Management

Log files are stored in the host's `./cron_logs` directory for easy viewing and debugging.

## Manual Testing

```bash
# Enter the container
docker-compose exec tdengine-coverage-cron bash

# Run the script manually
python3 /home/tdengine_coverage_alarm.py -test
```

## Troubleshooting

```bash
# Check cron service status
docker-compose exec tdengine-coverage-cron ps aux | grep cron

# View crontab configuration
docker-compose exec tdengine-coverage-cron crontab -l
```
