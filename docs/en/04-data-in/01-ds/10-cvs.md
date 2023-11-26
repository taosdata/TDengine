---
sidebar_label: CSV
title: CSV Data Source
description: This document describes how to extract data from CSV files into a TDengine Cloud instance.
---

CSV data source, which is written to the currently selected TDengine Cloud instance by uploading a CSV file to it.

## Prerequisites

- Create an empty database to store your CSV data. For more information, see [Database](../../../programming/model/#create-database).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **Kafka**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **CSV Options** column, you can set to ignore the first N rows, and you can enter a specific number.
4. In the **CSV Write Configuration** column, set the batch write amount, the default is 1000.
5. You can get the CSV file and parse it by **Upload CSV File** or **Configure CSV address**, which can be used to get the column information corresponding to the CSV:
      - Upload the CSV file or enter the address of the CSV file.
      - Select whether the package contains a header.
      - If the header is included, execute the next step directly to query the column information of the corresponding CSV and get the configuration information of the CSV.
      - In the case of not including Header, you need to input the customized column information, separated by commas, and then go to the next step to get the configuration information of the CSV.
      - CSV configuration items, each field needs to be configured: CSV column, DB column, column type (target), primary key (the whole configuration can only have one primary key, and the primary key must be of TIMESTAMP type), as a column, as a Tag. CSV columns are columns in this CSV file or customized columns; DB columns are columns of the corresponding data table
      - Subtable Naming Rule: used to configure the name of the subtable, using the format of "Prefix + {Column Type (Target)}", for example: d{id};
      - Super Table Name: used to configure the super table name when synchronizing to TDengine.
6. After completing the above information, click Submit button to start the data synchronization from CSV file to TDengine.
