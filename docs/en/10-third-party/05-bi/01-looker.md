---
title: Looker Studio
slug: /third-party-tools/analytics/looker-studio
---

Looker Studio, a powerful reporting and business intelligence tool under Google, was formerly known as Google Data Studio. At the Google Cloud Next conference in 2022, Google renamed it to Looker Studio. This tool offers a convenient data reporting experience with its rich data visualization options and diverse data connection capabilities. Users can easily create data reports based on preset templates to meet various data analysis needs.

Due to its easy-to-use interface and extensive ecosystem support, Looker Studio is favored by many data scientists and professionals in the field of data analysis. Whether a beginner or a seasoned analyst, users can quickly build attractive and practical data reports with Looker Studio, thereby better understanding business trends, optimizing decision-making processes, and enhancing overall operational efficiency.

## Acquisition

Currently, the TDengine connector, as a partner connector for Looker Studio, is now available on the Looker Studio official website. When users access the Data Source list in Looker Studio, they simply need to enter "TDengine" in the search to easily find and immediately use the TDengine connector.

The TDengine connector is compatible with both TDengine Cloud and TDengine Server data sources. TDengine Cloud is a fully managed IoT and industrial internet big data cloud service platform launched by Taos Data, providing users with a one-stop data storage, processing, and analysis solution; while TDengine Server is a locally deployed version that supports access via the public internet. The following content will introduce using TDengine Cloud as an example.

## Usage

The steps to use the TDengine connector in Looker Studio are as follows.

Step 1, after entering the details page of the TDengine connector, select TDengine Cloud from the Data Source dropdown list, then click the Next button to enter the data source configuration page. Fill in the following information on this page, then click the Connect button.

- URL and TDengine Cloud Token, which can be obtained from the instance list of TDengine Cloud.
- Database name and supertable name.
- Start and end times for querying data.
Step 2, Looker Studio will automatically load the fields and tags of the supertable under the configured TDengine database based on the configuration.
Step 3, click the Explore button in the upper right corner of the page to view the data loaded from the TDengine database.
Step 4, based on the needs, use the charts provided by Looker Studio to configure data visualization.

**Note** On the first use, please follow the prompts on the page to authorize access to Looker Studio's TDengine connector.
