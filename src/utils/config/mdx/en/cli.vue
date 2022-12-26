<template>
  <div>
    <p>
      The TDengine command-line interface (hereafter referred to as <code>TDengine CLI</code>) is the most simplest way for users to manipulate and
      interact with TDengine instances.
    </p>
    <h2 id="installation">Installation</h2>
    <p>
      To run TDengine CLI to access TDengine cloud, please install
      <a :href="installUrl">TDengine client installation package</a> first.
    </p>
    <h2 id="config">Config</h2>
    <el-tabs v-model="sysActivateTab" groupId="sys">
      <el-tab-pane name="linux" label="Config on Linux">
        <p>Run this command in your Linux terminal to save cloud DSN as variable:</p>
        <pre
          v-highlight="
            `export TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Config on Windows (coming soon)">
        <p>Run this command in your Windows terminal to save cloud DSN as variable:</p>
        <pre
          v-highlight="
            `set TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Config on Mac (coming soon)" groupId="sys">
        <p>Run this command in your Mac terminal to save cloud DSN as variable:</p>
        <pre
          v-highlight="
            `export TDENGINE_CLOUD_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="connect">Connect</h2>
    <el-tabs value="linux" groupId="sys">
      <el-tab-pane name="linux" label="Connect on Linux">
        <p>To access the TDengine Cloud, you can execute <code>taos</code> if you already set the environment variable.</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>
          If you did not set environment variable for a TDengine Cloud instance, or you want to access other TDengine Cloud instances rather than the
          instance you already set the environment variable, you can use <code>taos -E &lt;DSN&gt;</code> as below.
        </p>
        <pre v-highlight><code>taos -E $TDENGINE_CLOUD_DSN
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Connect on Windows (coming soon)">
        <p>To access the TDengine Cloud, you can execute <code>taos</code> if you already set the environment variable.</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>
          If you did not set environment variable for a TDengine Cloud instance, or you want to access other TDengine Cloud instances rather than the
          instance you already set the environment variable, you can use <code>taos -E &lt;DSN&gt;</code> as below.
        </p>
        <pre v-highlight><code>taos.exe -E $TDENGINE_CLOUD_DSN
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Connect on Mac (coming soon)">
        <p>To access the TDengine Cloud, you can execute <code>taos</code> if you already set the environment variable.</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>
          If you did not set environment variable for a TDengine Cloud instance, or you want to access other TDengine Cloud instances rather than the
          instance you already set the environment variable, you can use <code>taos -E &lt;DSN&gt;</code> as below.
        </p>
        <pre v-highlight><code>taos -E $TDENGINE_CLOUD_DSN
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="using-tdengine-cli">Using TDengine CLI</h2>
    <p>
      TDengine CLI will display a welcome message and version information if it successfully connected to the TDengine service. If it fails, TDengine
      CLI will print an error message. The TDengine CLI prompts as follows:
    </p>
    <pre v-highlight><code>Welcome to the TDengine shell from Linux, Client Version:3.0.0.0
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

Successfully connect to cloud.tdengine.com:8085 in restful mode

taos&gt;
</code></pre>
    <p>
      After entering the TDengine CLI, you can execute various SQL commands, including inserts, queries, or administrative commands. Please see the
      <a href="https://docs.tdengine.com/reference/taos-shell#execute-sql-script-file">official document</a> for more details.
    </p>
  </div>
</template>

<script>
  const installUrl = {
    linux: "https://www.tdengine.com/assets-download/3.0/TDengine-client-3.0.1.6-Linux-x64.tar.gz",
    windows: "https://www.tdengine.com/assets-download/3.0/TDengine-client-3.0.1.6-Windows-x64.exe",
    mac: "https://www.tdengine.com/assets-download/3.0/TDengine-client-3.0.1.6-macOS-x64.pkg",
  };
  export default {
    props: {
      token: {
        type: String,
        default: "",
      },
      url: {
        type: String,
        default: "",
      },
    },
    data() {
      return {
        sysActivateTab: "linux",
      };
    },
    computed: {
      jdbcURL() {
        return "jdbc:TAOS-RS://" + this.url.replace(/https?:\/\//, "") + "?usessl=" + this.url.startsWith("https") + "&token=" + this.token;
      },
      goDSN() {
        return (this.url.startsWith("https") ? "https" : "http") + "(" + this.url.replace(/https?:\/\//, "") + ")/?token=" + this.token;
      },
      DSN() {
        return this.url + "?token=" + this.token;
      },
      cloud_url() {
        return this.url;
      },
      cloud_token() {
        return this.token;
      },
      installUrl() {
        return installUrl[this.sysActivateTab];
      },
    },
    watch: {
      tokenList: {
        handler() {
          this.token = this.tokenList[0]?.token;
        },
      },
      immediate: true,
    },
  };
</script>
