---
sidebar_label: Edge Gateways
sidebar_position: 3.5
title: Edge Gateway Onboarding
description: Write data already collected by Neuron / NeuronEX edge gateways to TDengine with one click
---

import NeuronPrerequisites from '../../13-ecosystem-integrations/05-iot/resource/_neuron-prerequisites.mdx'
import NeuronMapping from '../../13-ecosystem-integrations/05-iot/resource/_neuron-mapping.mdx'
import NeuronOneClick from '../../13-ecosystem-integrations/05-iot/resource/_neuron-one-click.mdx'

## Overview

Edge gateway onboarding is designed for scenarios where device collection is already configured on a Neuron / NeuronEX gateway: in TDengine Explorer, on the **Data In → Edge Gateways** tab, enter the gateway address and credentials and confirm the mapping. Explorer then automatically configures the data stream, write rules, and collection subscriptions, and the collected data starts flowing continuously into the current cluster.

Unlike the other ingestion methods in this chapter, edge gateway onboarding does not create a taosX ingestion task and requires no parsing or mapping configuration — the device, tag, and data type definitions come directly from the gateway's existing collection configuration. The data plane is Neuron → eKuiper → taosAdapter, with supertables and subtables created automatically by schemaless writes. After configuration, the data path does not depend on Explorer staying online.

NeuronEX and the open-source Neuron + eKuiper combination are currently supported.

<NeuronPrerequisites />

<NeuronMapping />

<NeuronOneClick />

## Further Reading

- To define custom tags, business attributes, or data transformation, use [Manual Onboarding](../../13-ecosystem-integrations/05-iot/03-Neuron.md#manual-onboarding).
- For ingestion verification SQL, operation monitoring, Store-and-Forward, mTLS and high availability deployment, performance reference, and troubleshooting, see [Integration with Neuron](../../13-ecosystem-integrations/05-iot/03-Neuron.md).
