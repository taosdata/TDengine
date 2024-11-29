---
title: Looker Studio
slug: /third-party-tools/analytics/looker-studio
---

Looker Studio, as a powerful reporting and business intelligence tool under Google, was formerly known as Google Data Studio. At the Google Cloud Next conference in 2022, Google renamed it to Looker Studio. This tool provides users with a convenient data reporting generation experience, thanks to its rich data visualization options and diverse data connection capabilities. Users can easily create data reports based on pre-set templates to meet various data analysis needs.

Due to its user-friendly interface and vast ecosystem support, Looker Studio is favored by many data scientists and professionals in the field of data analysis. Whether for beginners or seasoned analysts, Looker Studio allows users to quickly build beautiful and practical data reports, thereby gaining better insights into business trends, optimizing decision-making processes, and improving overall operational efficiency.

## Access

Currently, the TDengine connector, as a partner connector for Looker Studio, is available on the Looker Studio website. When users access the Data Source list in Looker Studio, they can easily find and immediately use the TDengine connector by simply entering "TDengine" in the search bar.

The TDengine connector is compatible with two types of data sources: TDengine Cloud and TDengine Server. TDengine Cloud is a fully managed IoT and industrial IoT big data cloud service platform launched by Taos Data, providing users with a one-stop data storage, processing, and analysis solution; while TDengine Server is the locally deployed version that supports access via the public network. The following content will focus on TDengine Cloud as an example.

## Usage

The steps to use the TDengine connector in Looker Studio are as follows.

Step 1: After entering the details page of the TDengine connector, select TDengine Cloud from the Data Source dropdown list and click the Next button to enter the data source configuration page. Fill in the following information on this page, then click the Connect button.

- URL and TDengine Cloud Token, which can be obtained from the TDengine Cloud instance list.
- Database name and supertable name.
- Start time and end time for querying data.

Step 2: Looker Studio will automatically load the fields and tags of the configured supertables under the TDengine database based on the configuration.

Step 3: Click the Explore button at the top right of the page to view the data loaded from the TDengine database.

Step 4: Based on your needs, use the charts provided by Looker Studio to configure data visualization.

:::note

When using for the first time, please authorize access to the TDengine connector in Looker Studio according to the prompts on the page.

:::
