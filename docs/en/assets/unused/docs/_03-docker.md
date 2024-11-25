---
sidebar_label: Docker
title: Quickly Start TDengine with Docker
toc_max_heading_level: 4
---

This section provides a quick guide on how to start TDengine using Docker.

## Docker

1. If Docker is already installed on your machine, first pull the latest TDengine container image:
```bash
docker pull tdengine/tdengine:latest
```

Or pull a specific version of the container image:
```bash
docker pull tdengine/tdengine:3.3.0.0
```

2. Then simply execute the following command:
```bash
docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

**Note**: TDengine 3.0 server only uses port 6030 for TCP. Port 6041 is used by `taosAdapter` to provide REST services. Ports 6043-6049 are used by `taosAdapter` for third-party application access, and you can choose whether to open them based on your needs.

If you need to persist data to a specific folder on your machine, use the following command:
```bash
docker run -d -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

3. Verify that the container is running properly.
```bash
docker ps
```

4. Enter the container and run `bash`:
```bash
docker exec -it <container name> bash
```

Once inside, you can execute related Linux commands and access TDengine.

## Troubleshooting

If any issues occur when starting the TDengine service, check the database logs for more information. You can also refer to the troubleshooting section in the official TDengine documentation or seek help from the TDengine open-source community.
