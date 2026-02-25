# Use python:3-slim image for small size and good compatibility
FROM python:3-slim

# Replace Debian apt sources with Aliyun mirror for faster download
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# Install cron and necessary tools
RUN apt-get update && \
    apt-get install -y cron && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies using Aliyun PyPI mirror
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ requests beautifulsoup4 pandas

# Create log directory
RUN mkdir -p /var/log/cron

# Copy crontab file
COPY crontab /etc/cron.d/tdengine-coverage-cron

# Set crontab file permissions
RUN chmod 0644 /etc/cron.d/tdengine-coverage-cron && \
    crontab /etc/cron.d/tdengine-coverage-cron

# Create startup script
RUN echo '#!/bin/bash\n\
printenv | grep -v "no_proxy" >> /etc/environment\n\
echo "Starting cron service..."\n\
touch /var/log/cron/cron.log\n\
cron\n\
echo "Cron service started. Tailing logs..."\n\
tail -F /var/log/cron/cron.log' > /start.sh && \
    chmod +x /start.sh

# Start cron and keep container running
CMD ["/start.sh"]
