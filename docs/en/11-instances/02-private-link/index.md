---
title: PrivateLink
sidebar_label: PrivateLink
description: This document describes how to connect your VPC to TDengine Cloud with PrivateLink.
---

<!-- markdownlint-disable MD033 -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

PrivateLink is a highly available, scalable technology that you can use to privately connect your application to TDengine Cloud instance with different VPCs. PrivateLink allows the application in your VPC to connect to TDengine Cloud instance through your private IP addresses, and no needs to expose your data to the public internet.

Currently, TDengine Cloud supports private endpoint connections in AWS and GCP. Other clouds such as Azure will soon be supported.

The architecture of the PrivateLink is as follows:

![TDengine Cloud Architecture of PrivateLink](../../assets/private-link-01.webp)

For more details of the PrivateLink concept, please see the following documents:  
[What is AWS PrivateLink?](https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html)  
[AWS PrivateLink concepts](https://docs.aws.amazon.com/vpc/latest/privatelink/concepts.html)  
[GCP Private Service Connect](https://cloud.google.com/vpc/docs/private-service-connect)

## How to use PrivateLink

### Step 1: choose a endpoint service name in TDengine Cloud

1. Open [Instances](https://cloud.tdengine.com/instances/privateLink) page in TDengine Cloud, Click **Service List** button.
2. Record the **Service Name** from the list of services in the correct cloud and region for later usage.

### Step 2: Set up a private endpoint

<Tabs defaultValue="AWS">
<TabItem value="AWS" label="AWS">

To use the AWS Management Console to create a VPC interface endpoint, please follow these steps:

1. Sign in to the [AWS Management Console](https://aws.amazon.com/console/) and open the Amazon VPC console at [AWS VPC](https://console.aws.amazon.com/vpc/).
2. Select the region where your VPC is located from the drop-down list in the upper-right corner. Find **Virtual private cloud** in the left navigation pane and Click **Endpoints**, and then click **Create Endpoint** in the upper-right corner. The **Create endpoint** page is displayed.

   ![TDengine Cloud Create endpoint 1](../../assets/private-link-02.webp)

3. Select Other endpoint services.
4. Enter the service name that you choose in **Step 1**. Click **Verify service**.
5. Select your VPC in the drop-down list.
6. In the Subnets area, select all the availability zones, and select the Subnet ID.
7. Select your security group properly in the Security groups area.

   :::note IMPORTANT
   
   Make sure the selected security group allows inbound access from your EC2 instances on port 443.

   :::

8. Click Create endpoint. Then you have the **VPC endpoint ID**.

</TabItem>
<TabItem value="GCP" label="GCP">

1. Sign in to the [GCP Private Service Connect](https://console.cloud.google.com/net-services/psc/list/consumers).
2. Find **CONNECTED ENDPOINTS** tab and then click **+CONNECT ENDPOINT**.
3. Select **Published service**.
4. Enter the service name that you choose in **Step 1** in the **Target service**.
5. Enter a name in the **Endpoint name**.
6. In the Subnets area, select the Network, and select the Subnetwork.
7. Create a Reserve a static internal IP address for the endpoint.
8. Click ADD ENDPOINT. Then you have the **PSC Connection ID**.

</TabItem>
</Tabs>

### Step 3: Create endpoint connection using TDengine Cloud

1. In TDengine Cloud left navigation panel, select **Instance**, then choose the **PrivateLink** tab, click **Add New Private Link** in the upper-right corner. The **Add New Private Link** page will be displayed.
2. Input your preferred connection name, select the correct endpoint service to use to create the connection, and then enter the **Endpoint ID** you created in step 2.
3. Click the **Verify** button to verify if the request existed and could be accepted.
4. Click the **Add** button to create the private endpoint connection.
5. You can find the connection info in the connection list in PrivateLink page.
6. Wait a few minutes, then refresh the page to see the connection status is CONNECTED.
7. The connection have three statuses: CONNECTED, DISCONNECTED and PENDING. When the operation is in progress, the connection status is PENDING. You need to wait a few minutes for the operation to complete.

### Step 4: Enable private DNS names

<Tabs defaultValue="AWS">
<TabItem value="AWS" label="AWS">

1. Click the endpoint id link in **Endpoints** page you created in Step 2.
2. Click **Actions** in the upper-right area of the page and then select **Modify private DNS name**.
3. Check the box **Enable for this endpoint** and then click **Save changes**.
4. Then you can find the **Private DNS names** shown in **Endpoint Details** page.

</TabItem>
<TabItem value="GCP" label="GCP">

1. Sign in to the [GCP Cloud DNS](https://console.cloud.google.com/net-services/dns/zones).
2. Click **CREATE ZONE** to create a new zone and then select **Private** of Zone type.
3. Enter a name in the **Zone name** and the **Private DNS Name** that you choose in **Step 1** in the **DNS Name**.
4. Click **CREATE** to create a new zone.
5. Select the zone you created in the **Cloud DNS** page and click **ADD STANDARD** to add a new record set.
6. Choose the static internal IP address of **Step 2** in the **IPv4 Address** and click **CREATE** to create a new standard.

</TabItem>
</Tabs>

### Step 5: Using the private DNS name to call TDengine Cloud Service

Now you can access TDengine Cloud instance in your VPC using the private DNS name. You can find the **Private URL** in the [Instances page](https://cloud.tdengine.com/instances) in TDengine Cloud.

## How to remove the endpoint connection

<Tabs defaultValue="AWS">
<TabItem value="AWS" label="AWS">

1. Click the **Actions** button in **endpoint connection list** page in TDengine Cloud. After a while, the connection status will be changed into DISCONNECTED.
2. Delete the connection in  **endpoint connection list** page in TDengine Cloud.
3. Remove the private endpoint in the AWS Console. Otherwise, AWS will continue to charge.
   1. Select the endpoint in **Endpoints** page in AWS.  
   2. Click **Actions**  in the upper-right area of the page and then select **Delete VPC Endpoints**.

</TabItem>
<TabItem value="GCP" label="GCP">

1. Click the **Actions** button in **endpoint connection list** page in TDengine Cloud. After a while, the connection status will be changed into DISCONNECTED.
2. Delete the connection in  **endpoint connection list** page in TDengine Cloud.
3. Remove the private endpoint in the GCP Console. Otherwise, GCP will continue to charge.
   1. Select the endpoint in **Network services** -> **Private Service Connect** -> **CONNECTED ENDPOINTS** page in GCP.  
   2. Click **Actions**  in the upper-right area of the page and then select **Delete**.

</TabItem>
</Tabs>
