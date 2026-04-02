<template>
  <div class="docs">
    <h2 id="perspective-introduction">{{ $t('tools.perspective.step1full') }}</h2>
    <p><a :href="`https://www.perspective.co/`">Perspective</a> {{ $t('tools.perspective.desc') }}</p>
    <p>{{ $t('tools.perspective.desc1') }}</p>

    <h2 id="perspective-prepare">{{ $t('tools.perspective.step2full') }}</h2>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.perspective.step2desc1') }}
        <a :href="`https://python.org/`">{{ $t('tools.perspective.step2desc2') }}</a
        >{{ $t('tools.perspective.step2desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step2desc4') }}
        <pre
          v-highlight="
            `pip3 install taospy
pip3 install taos-ws-py`
          "
        ><code class="language-bash"></code></pre>
      </li>
    </ol>

    <h2 id="perspective-import">{{ $t('tools.perspective.step3full') }}</h2>
    <p>{{ $t('tools.perspective.step3desc') }}</p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.perspective.step3desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step3desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step3desc3') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step3desc4') }}
        <pre v-highlight><code class="language-python">
def perspective_thread(perspective_server: perspective.Server, tdengine_conn: taosws.Connection):
    """
    Create a new Perspective table and update it with new data every 50ms
    """
    # create a new Perspective table
    client = perspective_server.new_local_client()
    schema = {
        "timestamp": datetime,
        "location": str,
        "groupid": int,
        "current": float,
        "voltage": int,
        "phase": float,
    }
    # define the table schema
    table = client.table(
        schema,
        limit=1000,                     # maximum number of rows in the table
        name=PERSPECTIVE_TABLE_NAME,    # table name. Use this with perspective-viewer on the client side
    )
    logger.info("Created new Perspective table")

    # update with new data
    def updater():
        data = read_tdengine(tdengine_conn)
        table.update(data)
        logger.debug(f"Updated Perspective table: {len(data)} rows")

    logger.info(f"Starting tornado ioloop update loop every {PERSPECTIVE_REFRESH_RATE} milliseconds")
    # start the periodic callback to update the table data
    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=PERSPECTIVE_REFRESH_RATE)
    callback.start()
        </code><p><a :href="`https://github.com/taosdata/TDengine/blob/main/docs/examples/perspective/perspective_server.py`">{{ $t('tools.perspective.step3desc5') }}</a></p></pre>
      </li>
    </ol>

    <h2 id="perspective-viewer">{{ $t('tools.perspective.step4full') }}</h2>
    <p>{{ $t('tools.perspective.step4desc') }}</p>

    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.perspective.step4desc1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step4desc2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step4desc3') }}
        <pre v-highlight><code class="language-html">
&lt;script type="module"&gt;
    // import the Perspective library
    import perspective from "https://unpkg.com/@finos/perspective@3.1.3/dist/cdn/perspective.js";

    document.addEventListener("DOMContentLoaded", async function () {
        // an asynchronous function for loading the view
        async function load_viewer(viewerId, config) {
            try {
                const table_name = "meters_values";
                const viewer = document.getElementById(viewerId);
                // connect WebSocket server
                const websocket = await perspective.websocket("ws://localhost:8085/websocket");
                // open server table
                const server_table = await websocket.open_table(table_name);
                // load the table into the view
                await viewer.load(server_table);
                // use view configuration
                await viewer.restore(config);
            } catch (error) {
                console.error('发生错误:', error);
            }
        }

        // configuration of the view
        const config1 = {
            "version": "3.3.1",          // Perspective library version (compatibility identifier)
            "plugin": "Datagrid",        // View mode: Datagrid (table) or D3FC (chart)
            "plugin_config": {           // Plugin-specific configuration
                "columns": {
                    "current": {
                        "width": 150       // Column width in pixels
                    }
                },
                "edit_mode": "READ_ONLY",  // Edit mode: READ_ONLY (immutable) or EDIT (editable)
                "scroll_lock": false       // Whether to lock scroll position
            },
            "columns_config": {},        // Custom column configurations (colors, formatting, etc.)
            "settings": true,            // Whether to show settings panel (true/false)
            "theme": "Power Meters",     // Custom theme name (must be pre-defined)
            "title": "Meters list data", // View title
            "group_by": ["location", "groupid"], // Row grouping fields (equivalent to `row_pivots`)
            "split_by": [],              // Column grouping fields (equivalent to `column_pivots`)
            "columns": [                 // Columns to display (in order)
                "timestamp",
                "location",
                "current",
                "voltage",
                "phase"
            ],
            "filter": [],                // Filter conditions (triplet format array)
            "sort": [],                  // Sorting rules (format: [field, direction])
            "expressions": {},           // Custom expressions (e.g., calculated columns)
            "aggregates": {              // Aggregation function configuration
                "timestamp": "last",       // Aggregation: last (takes the latest value)
                "voltage": "last",         // Aggregation: last
                "phase": "last",           // Aggregation: last
                "current": "last"          // Aggregation: last
            }
        };

        // load the first view
        await load_viewer("prsp-viewer-1", config1);
    });
&lt;/script&gt;

&lt;!-- define the HTML Structure of the Dashboard --&gt;
&lt;div id="dashboard"&gt;
    &lt;div class="viewer-container"&gt;
       &lt;perspective-viewer id="prsp-viewer-1" theme="Pro Dark"&gt;&lt;/perspective-viewer&gt;
    &lt;/div&gt;
&lt;/div&gt;
        </code><p><a :href="`https://github.com/taosdata/TDengine/blob/main/docs/examples/perspective/prsp-viewer.html`">{{ $t('tools.perspective.step4desc4') }}</a></p></pre>
      </li>
      <li class="odbc-span">
        {{ $t('tools.perspective.step4desc5') }}
        <a :href="`${TdDocsUrl}/third-party/visual/perspective`">{{ $t('tools.perspective.step4desc6') }}</a
        >{{ $t('tools.perspective.step4desc7') }}
      </li>
    </ol>
  </div>
</template>

<script lang="ts" setup>
import { TdDocsUrl } from 'config';
</script>
<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}

.odbc-ol {
  padding-left: 0;

  .odbc-span {
    padding-left: 20px;
    line-height: 30px;
    text-indent: -20px;

    .pre-code {
      text-indent: 0;

      // margin-left: -17px;
    }
  }
}

.gf-input {
  display: flex;
  align-items: center;
  width: 70%;
  margin: 10px;
  margin-left: 0;
  font-size: 14px;
  text-indent: 0;

  .gf-input-label {
    width: 135px;
  }

  .gf-input-value {
    flex: 1;
    padding: 0 10px;
    line-height: 40px;
    background-color: #f6f8fa;
    border-radius: 5px;
  }
}
</style>
