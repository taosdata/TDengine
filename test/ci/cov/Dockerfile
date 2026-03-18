# Use python:3-slim image for small size and good compatibility
FROM python:3-slim

# Copy crontab file early for better layer caching
COPY crontab /etc/cron.d/tdengine-coverage-cron

# Optimize image size by combining all RUN commands into one layer
RUN set -eux; \
    # Replace Debian apt sources with Aliyun mirror for faster download
    sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources; \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources; \
    \
    # Install cron and clean up in the same layer to reduce image size
    apt-get update; \
    apt-get install -y --no-install-recommends cron; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    \
    # Install Python dependencies using Aliyun PyPI mirror
    pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ \
        requests beautifulsoup4 pandas; \
    \
    # Create log directory and set crontab permissions
    mkdir -p /var/log/cron; \
    chmod 0644 /etc/cron.d/tdengine-coverage-cron; \
    \
    # Create startup script
    printf '%s\n' \
        '#!/bin/bash' \
        'printenv | grep -v "no_proxy" >> /etc/environment' \
        'echo "Starting cron service..."' \
        'touch /var/log/cron/cron.log' \
        'cron' \
        'echo "Cron service started. Container will run indefinitely."' \
        'echo "To view logs: docker-compose exec tdengine-coverage-cron cat /var/log/cron/cron.log"' \
        '' \
        'tail -f /dev/null' \
        > /start.sh; \
    chmod +x /start.sh

# Start cron and keep container running
CMD ["/start.sh"]
