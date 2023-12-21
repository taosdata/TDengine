---
title: Connect JupyterLab to TDengine
sidebar_label: JupyterLab
description: This document describes how to integrate TDengine with JupyterLab.
---

JupyterLab is the next generation of the ubiquitous Jupyter Notebook. In this note we show you how to install the TDengine Python client library to connect to TDengine in JupyterLab. You can then insert data and perform queries against the TDengine instance within JupyterLab.

## Install JupyterLab
Installing JupyterLab is very easy. Installation instructions can be found at:  

https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html.  

If you don't feel like clicking on the link here are the instructions.  
Jupyter's preferred Python package manager is pip, so we show the instructions for pip.  
You can also use **conda** or **pipenv** if you are managing Python environments.
````
pip install jupyterlab
````

For **conda** you can run:
````
conda install -c conda-forge jupyterlab
````

For **pipenv** you can run:
````
pipenv install jupyterlab
pipenv shell
````

## Run JupyterLab
You can start JupyterLab from the command line by running:
````
jupyter lab
````
This will automatically launch your default browser and connect to your JupyterLab instance, usually on port 8888.

## Install the TDengine Python client library
You can now install the TDengine Python client library as follows.  

Start a new Python kernel in JupyterLab.  

If using **conda** run the following:
````
# Install a conda package in the current Jupyter kernel
import sys
!conda install --yes --prefix {sys.prefix} taospy
````
If using **pip** run the following:
````
# Install a pip package in the current Jupyter kernel
import sys
!{sys.executable} -m pip install taospy
````

## Connect to TDengine
You can find detailed examples to use the Python client library, in the TDengine documentation here.
Once you have installed the TDengine Python client library in your JupyterLab kernel, the process of connecting to TDengine is the same as that you would use if you weren't using JupyterLab.
Each TDengine instance, has a database called "log" which has monitoring information about the TDengine instance.
In the "log" database there is a [supertable](https://docs.tdengine.com/taos-sql/stable/) called "disks_info".  

The structure of this table is as follows:
````
taos> desc disks_info;
             Field              |         Type         |   Length    |   Note   |
=================================================================================
 ts                             | TIMESTAMP            |           8 |          |
 datadir_l0_used                | FLOAT                |           4 |          |
 datadir_l0_total               | FLOAT                |           4 |          |
 datadir_l1_used                | FLOAT                |           4 |          |
 datadir_l1_total               | FLOAT                |           4 |          |
 datadir_l2_used                | FLOAT                |           4 |          |
 datadir_l2_total               | FLOAT                |           4 |          |
 dnode_id                       | INT                  |           4 | TAG      |
 dnode_ep                       | BINARY               |         134 | TAG      |
Query OK, 9 row(s) in set (0.000238s)
````

The code below is used to fetch data from this table into a pandas DataFrame.

````
import sys
import taos
import pandas

def sqlQuery(conn):
    df: pandas.DataFrame = pandas.read_sql("select * from log.disks_info limit 500", conn)
    print(df)
    return df

conn = taos.connect()

result = sqlQuery(conn)

print(result)
````

TDengine has client libraries for various languages including Node.js, Go, PHP and there are kernels for these languages which can be found [here](https://github.com/jupyter/jupyter/wiki/Jupyter-kernels).
