---
sidebar_label: Deploy TDengine Enterprise
title: Get Started with TDengine Enterprise
slug: /get-started/deploy-enterprise-edition
---

## Prerequisites

- Ensure that your servers are running Ubuntu 18.04, CentOS 7.9 or later.
- Ensure that `systemd` is installed and enabled on all servers.
- Confirm the location of all storage media that you want to use for tiered storage.
- If you want to use shared storage, such as S3, prepare the access string for the desired storage device.
- If you want to try TDengine Enterprise using Docker, please refer to [Get Started with TDengine Using Docker](../deploy-in-docker/) or [docker hub](https://hub.docker.com/r/tdengine/tdengine-ee).

## Prepare Your Environment

The following items are components of TDengine and are installed automatically when you
deploy TDengine. By default, these all run on the same machine.

- [taosd](../../tdengine-reference/components/taosd/): the TDengine server
- [taosc](../../tdengine-reference/components/taosc/): the TDengine client
- [taosAdapter](../../tdengine-reference/components/taosadapter/): the REST API for TDengine
- [taosX](../../tdengine-reference/components/taosx/): the ETL engine for TDengine, supporting data ingestion and export.
- [taosKeeper](../../tdengine-reference/components/taoskeeper/): the metrics collector for TDengine
- [taosExplorer](../../tdengine-reference/components/taosexplorer/): the web interface for TDengine

You may want to install several additional components to support your deployment. These include:

- [TDgpt](../../advanced/tdgpt/): the data-analysis platform and AI assistant for TDengine.
- [TDinsight](../../tdengine-reference/components/tdinsight/): the monitoring solution for TDengine, based on Grafana.

## Install TDengine

This section describes how to install TDengine Enterprise on a bare metal machine. Note that the same instructions also apply to virtual machines.

1. Transfer the installation package for TDengine Enterprise to the target machine. The installation package is provided along with the delivery of your TDengine Enterprise license. The package name is in the format `TDengine-enterprise-<version>-Linux-<arch>.tar.gz`.

2. Decompress the installation package.

    ```bash
    tar xf TDengine-enterprise-<version>-Linux-<arch>.tar.gz
    ```

3. Open the directory created and run the installation script.

    ```bash
    cd TDengine-enterprise-<version>/
    ./install.sh
    ```

4. Enter a hostname or IP address for the local machine that can be accessed by any applications required to interoperate with TDengine.

5. Enter the FQDN of the TDengine cluster that you want the local machine to join. If the local machine is intended to be the first node in a new TDengine cluster, leave the field blank and press Enter.

6. When prompted to enter an email address, leave the field blank and press Enter.

TDengine Enterprise has now been installed.

Important: Do not start TDengine at this time. First proceed with the configuration items below.

## Configure TDengine Enterprise

Before starting TDengine, open the `/etc/taos/taos.cfg` file and configure the following database options:

1. Logging: Uncomment the `logDir` parameter and set it to the directory where you want to store TDengine log files.

    For example:

    ```conf
    logDir /data/log/
    ```

2. Tiered Storage: Uncomment the `dataDir` parameter and specify each mount point that you want to use in tiered storage. Include one `dataDir` entry for each mount point in the following format:

    ```conf
    dataDir <path> <tier> <primary>
    ```

    For example, the following configuration indicates that `/var/lib/taos` is tier 0 storage and is primary, `/var/lib/taos1` is tier 1 storage, and `/var/lib/taos2` is tier 2 storage.

    ```conf
    dataDir /var/lib/taos 0 1
    dataDir /var/lib/taos1 1 0
    dataDir /var/lib/taos2 2 0
    ```

    Note that each cluster can have only one primary storage device, and the primary storage device must be on tier 0.

    TDengine also supports using cloud providers' object storage as storage device. However, since most object storage systems already have multiple replicas, enabling TDengine's multi-replica feature on top would lead to wasted storage space and increased costs. To address this, TDengine has further improved object storage into shared storage—where the storage device logically maintains only one copy of data that is shared across all nodes in the TDengine cluster. This approach significantly reduces both storage space requirements and costs.
    
    To enable shared storage, please add the following parameters to the configuration file:

    ```conf
    ssEnabled 2
    ssAccessString s3:endpoint=s3.amazonaws.com;bucket=mybucket;uriStyle=path;protocol=https;accessKeyId=AKMYACCESSKEY;secretAccessKey=MYSECRETACCESSKEY;region=us-east-2;chunkSize=64;maxChunks=10000;maxRetry=3
    ssUploadDelaySec 10
    ssAutoMigrateIntervalSec 600
    ssPageCacheSize 1
    ```

    The possible values for `ssEnabled` are `0`, `1` and `2`. `0` is the default, which means shared storage is disabled; `1` means only enable manual migration(migrate local data to shared storage), and `2` means also enable auto migration. 

    The format of `ssAccessString` is `<device-type>:<option-name>=<option-value>;<option-name>=<option-value>;...`.
    
    When using Amazon S3 compatible object storage as shared storage, the `device-type` should be `s3`, and the following table list all possible options:

    Name            |   Description
    ----------------|----------------------------------------------
    endpoint        | host name / ip address, and an optional port number of the object storage server.
    bucket          | bucket name.
    protocol        | `https` or `http`, `https` is the default.     
    uriStyle        | `virtualHost` or `path`, `virtualHost` is the default, but please note that some object storage provider only support `path`.
    region          | object storage service region, optional.
    accessKeyId     | your access key id.              
    secretAccessKey | your secret access key.
    chunkSize       | chunk size in MB, files larger than this size will use multipart upload, default is 64.
    maxChunks       | max number of allowed chunks in a multipart upload, default is 10000.
    maxRetry        | max retry times when encounter retryable errors, default is 3, negative value means unlimited retry.

    When using locally mounted storage device as shared storage, the `device-type` should be `fs`, and the following table list all possible options:

    Name           |   Description
    ---------------|----------------------------------------------
    baseDir        | path of a directory, TDengine will use this directory as shared storage.


3. (Optional) Modify the `/etc/taos/taosadapter.toml` file as follows to enable SSL on taosAdapter:

    ```toml
    [ssl]
    enable = true
    certFile = "<your-certificate.crt>"
    keyFile = "<your-private-key.pem>"
    ```

    Note that the certificate and key files must be in PEM format.

4. (Optional) If you want to configure global options for the data ingestion task runner, modify the /etc/taos/taosx.toml file accordingly.

## Start TDengine

To start all TDengine components, run the `start-all.sh` command:

```bash
sudo ./start-all.sh
```

You can also start components individually:

```bash
sudo systemctl start taosd
sudo systemctl start taosadapter
sudo systemctl start taoskeeper
sudo systemctl start taosx
sudo systemctl start taos-explorer
```

If you want to stop TDengine, you can run the stop-all.sh command to stop all services or use `systemctl stop <component-name>` to stop a specified service.

## Change the Root Password

When you deploy TDengine, the `root` user is created. The default password for this user is `taosdata`. For security purposes, it is recommended that you change this password.

1. In a web browser, open the taosExplorer interface for TDengine. This interface is located on port 6060 on the hostname or IP address running TDengine.

2. Enter root as the username and taosdata as the password and click **Sign In**.

3. Hover over the **T** icon in the top right and click **Change Password**.

4. Enter your new password. Note the following restrictions:

    1. The password must be between 8 and 16 characters, inclusive.

    2. The password must contain at least three of the following character types: uppercase letters, lowercase letters, digits, and special characters.

    3. The password cannot contain single quotation marks (`'`), double quotation marks (`"`), backticks (`` ` ``), backslashes (`\`), or spaces.

5. Click Save Changes.

The root password has been changed. Ensure that you keep this password secure.

## Activate Your Enterprise License

1. In a web browser, open taosExplorer and log in as a TDengine user.

2. In the main menu on the left, click **Management** and open the License tab.

3. Record the **Cluster ID**, located under Basic Database Parameters, and provide this value to your account manager. A license code will be generated by the TDengine team and delivered to you.

4. Once you obtain your license code, click **Activate License** and enter your code.

5. Click **Confirm**.

Your license has been activated and you can test enterprise features.

## Monitor TDengine

The taosKeeper component collects monitoring metrics for TDengine. This component is installed and enabled automatically when you install TDengine.

To use TDinsight, a Grafana-based monitoring solution, to monitor your TDengine cluster, see [Monitoring Your Cluster](../../operations-and-maintenance/monitor-your-cluster/) in the official documentation.
