<template>
  <div class="terminal-wrapper">
    <div id="terminal" @contextmenu="contextmenu" @mousedown="mousedown"></div>
  </div>
</template>

<script>
  import { CustomShellContent } from "@/const";
  import { Terminal } from "xterm";
  import { FitAddon } from "xterm-addon-fit";
  import "xterm/css/xterm.css";
  import { WebglAddon } from "xterm-addon-webgl";
  import { SearchAddon } from "xterm-addon-search";
  import { SerializeAddon } from "xterm-addon-serialize";
  import { table } from "table";
  import { connect } from "@tdengine/websocket";
  import { copy } from "@/utils";
  let term = null;
  const serializeAddon = new SerializeAddon();
  let command = "";
  const keyChar = {
    up: "\x1B[A",
    down: "\x1B[B",
    left: "\x1B[D",
    right: "\x1B[C",
  };
  export default {
    name: "",
    mixins: [],
    components: {},
    props: {},
    data() {
      this.historyKey = "shell_history_" + this.$store.getters.appId;
      this.history = JSON.parse(localStorage.getItem(this.historyKey) || "[]");
      return {
        term: "", // 保存terminal实例
        rows: 40,
        cols: 100,
        consoleLoading: false,
        sshPrompt: "",
        ws: null,
        loading: false,
        result: [],
        currentOutput: "",
        wsFailed: false,
        shellData: [],
        promptStr: "$ ",
        currentUseDB: "",
      };
    },
    computed: {
      current_cluster() {
        return this.$store.state.app.current_cluster;
      },
      wsurl() {
        // return "ws://gw.ali.cloud.taosdata.com:8080/rest/ws?token=a6a0f770213fb9706f5eb9ad24aaf5020f7b8d79";
        // return `${this.current_cluster.gateway_url.replace(/^http/, "ws")}/rest/ws?token=${this.current_cluster.token.token}`;
        return `${'https://gw.us-east-1.aws.cloud.tdengine.com'.replace(/^http/, "ws")}/rest/ws?token=c7063aa13fb703585f6a0a444652be829f826d1b`;
      },
      useDB() {
        return this.$store.state.console.useDB;
      },

      command: {
        get: function () {
          return this.$store.state.console.sqlStr;
        },
        set: function (val) {
          this.$store.commit("console/SET_SQLSTR", val);
        },
      },
      addSql() {
        return this.$store.state.console.addSql;
      },
    },
    watch: {
      addSql(newVal) {
        if (newVal) {
          this.addSqlVal(newVal);
        }
      },
      wsFailed(newVal) {
        if (newVal && term) {
          term.setOption("disableStdin", true);
        }
      },
    },
    created() {
      this.initWs();
    },
    methods: {
      initWs() {
        this.loading = true;
        this.ws = connect(this.wsurl);
        this.ws
          .connect()
          .then(() => {
            this.$BusOnAndAutoOff("console/useDB", dbname => {
              const sql = `use ${dbname};`;
              if (sql == this.currentUseDB) return;
              command && term.prompt();
              command = sql;
              this.currentUseDB = sql;
              term.write(`use ${dbname};`);
              this.enter();
            });
          })
          .catch(() => {
            this.wsFailed = true;
            this.$error("webscocket connect error");
          })
          .finally(() => {
            this.initTerm();
            this.loading = false;
          });
      },

      // 初始化终端
      initTerm() {
        if (term) return;
        this.consoleLoading = true;
        let termContainer = document.getElementById("terminal");
        term = new Terminal({
          disableStdin: this.wsFailed,
          cursorBlink: true,
          cursorStyle: "underline",
          rightClickSelectsWord: true,
          // rendererType: "canvas",
          screenReaderMode: true,
          fontFamily: '"Cascadia Code", Menlo, monospace',
          rows: Math.floor(window.innerHeight / 25.25),
          cols: Math.floor(window.innerWidth / 10.5),
          allowProposedApi: true,
          theme: {
            foreground: "#eff0eb",
            background: "#282a36",
            selection: "#97979b33",
            black: "#282a36",
            brightBlack: "#686868",
            red: "#ff5c57",
            brightRed: "#ff5c57",
            green: "#5af78e",
            brightGreen: "#5af78e",
            yellow: "#f3f99d",
            brightYellow: "#f3f99d",
            blue: "#57c7ff",
            brightBlue: "#57c7ff",
            magenta: "#ff6ac1",
            brightMagenta: "#ff6ac1",
            cyan: "#9aedfe",
            brightCyan: "#9aedfe",
            white: "#f1f1f0",
            brightWhite: "#eff0eb",
          },
        });
        let fitAddon = new FitAddon();
        term.loadAddon(fitAddon);
        term.open(termContainer);

        const searchAddon = new SearchAddon();
        const addon = new WebglAddon();
        addon.onContextLoss(() => {
          addon.dispose();
        });
        term.loadAddon(addon);
        term.loadAddon(searchAddon);
        term.loadAddon(serializeAddon);
        this.writeCustomContent();
        this.initHistory();
        term.prompt = () => {
          command = "";
          term.write("\r\n" + this.promptStr);
        };
        term.write(this.promptStr);
        fitAddon.fit();
        term.scrollToBottom();
        // 内容全屏显示-窗口大小发生改变时
        const resizeScreen = () => {
          try {
            fitAddon.fit();
            term.scrollToBottom();
          } catch (e) {
            console.log(e);
          }
        };
        window.addEventListener("resize", resizeScreen);
        this.$once("hook:beforeDestroy", () => {
          window.removeEventListener("resize", resizeScreen);
        });
        term.focus(); // 光标聚焦
        this.addDataEvent();
        this.addKeyEvent();
        this.$BusOnAndAutoOff("console/xterm/focus", () => {
          this.$nextTick(() => {
            fitAddon.fit();
            term.scrollToBottom();
            term.focus();
          });
        });
      },
      addKeyEvent() {
        term.onKey(async ({ domEvent }) => {
          // 复制
          // if (domEvent.key == "c" && (domEvent.ctrlKey || domEvent.composed) && term.hasSelection()) {
          //   // this.copy(term.getSelection());
          // }
          // console.log(domEvent.keyCode);
          let commands, index;
          switch (domEvent.keyCode) {
            case 13:
              if (domEvent.shiftKey) {
                command += "\n";
                term.write("\n");
              } else {
                await this.enter();
              }
              break;
            case 38:
              // 向上方向
              commands = localStorage.getItem("commands") ? JSON.parse(localStorage.getItem("commands")) : [];
              index = localStorage.getItem("index") ? localStorage.getItem("index") : commands.length;
              index = parseInt(index);
              if (commands.length && index < commands.length + 1 && index > 0) {
                // 删除现有命令
                term.write("\b \b".repeat(command.length));
                command = commands[index - 1];
                term.write(command);
                localStorage.setItem("index", index - 1);
              }
              break;
            case 40:
              // 向下方向
              commands = localStorage.getItem("commands") ? JSON.parse(localStorage.getItem("commands")) : [];
              index = localStorage.getItem("index") ? localStorage.getItem("index") : commands.length;
              index = parseInt(index);
              if (commands.length && index < commands.length - 1 && index > -1) {
                let position = this.getCursorPosition();
                for (let currentPos = position; currentPos < command.length; currentPos++) {
                  term.write(keyChar.right);
                }
                // 删除现有命令
                term.write("\b \b".repeat(command.length));
                command = commands[index + 1];
                term.write(command);
                localStorage.setItem("index", index + 1);
              }
              break;
            case 37:
              // 向左方向
              if (this.getCursorPosition()) {
                term.write(keyChar.left);
              }
              break;
            case 39:
              // 向右方向
              term.write(keyChar.right);
              break;
            case 8:
              //退格
              if (term._core.buffer.x > 2) {
                // this.write("\x7f");
                const index = this.getCursorPosition();
                if (command.length > 0) {
                  const lastStr = command.slice(index);
                  const firstStr = command.slice(0, index - 1);
                  command = firstStr + lastStr;
                  term.write("\x1b[2K\r");
                  this.write(this.promptStr + command + keyChar.left.repeat(lastStr.length));
                }
              }
              break;
            default:
              break;
          }
        });
        term.attachCustomKeyEventHandler(domEvent => {
          let keydown = domEvent.type === "keydown";
          // 粘贴
          if (domEvent.key == "v" && (domEvent.ctrlKey || domEvent.metaKey) && keydown) {
            term.paste("");
            return false;
          }
          // 复制
          if (domEvent.key == "c" && (domEvent.ctrlKey || domEvent.metaKey) && keydown && term.hasSelection()) {
            return false;
          }
        });
      },
      async enter() {
        command = command.trim();
        if (command.length === 0) {
          command = "";
          return term.prompt();
        } else {
          // 保存命令
          let commands = localStorage.getItem("commands") ? JSON.parse(localStorage.getItem("commands")) : [];
          commands.push(command);
          localStorage.setItem("commands", JSON.stringify(commands));
          localStorage.setItem("index", commands.length);
        }
        switch (command) {
          case "clear":
            command = "";
            // term.clear();
            this.history = [];
            localStorage.removeItem(this.historyKey);
            term.reset();
            term.prompt();
            break;
          case "exit":
            term.dispose();
            break;
          default:
            await this.sendData();
        }
        command = "";
        term.focus();
      },
      addDataEvent() {
        // 添加事件监听器，支持输入方法
        term.onData(async key => {
          if ((key >= String.fromCharCode(0x20) && key <= String.fromCharCode(0x7e)) || key >= "\u00a0") {
            let index = this.getCursorPosition();
            let currentStr = command.slice(index) || "";
            command = command.slice(0, index) + key + currentStr;
            let leftStr = keyChar.left.repeat(currentStr.length);
            this.write(key + currentStr + leftStr);
          }
        });
      },
      async sendData() {
        this.shellData = [];
        await this.ws
          .query(command)
          .then(data => {
            if (data.timing) {
              data.timing = (parseInt(data.timing) / 1000000).toFixed(5);
            }
            this.success(data);
          })
          .catch(err => {
            this.error(err);
          });
        this.write("\r\n");
        this.writeShell(this.currentOutput, false);
        term.prompt();
      },
      success(res) {
        if (!res.data) {
          this.currentUseDB = command;
          this.setHistory({
            createdAt: Date.now(),
            time: res.timing,
            cluster: this.current_cluster.name,
            // database: state.useDB,
            sql: command,
            type: 0,
            rows: 0,
            message: `Query OK, ${res.affectRows} rows affected (${res.timing || 0} ms)`,
            appId: this.current_cluster.id,
          });
        } else {
          let record = {
            createdAt: Date.now(),
            time: res.timing,
            cluster: this.current_cluster.name,
            // database: state.useDB,
            sql: command,
            type: 1,
            rows: res.data?.length,
            message: "success",
            appId: this.current_cluster.id,
          };
          let data = res.data.map(item => item.map(val => val + ""));
          let head = res.meta.map(item => item.name);
          this.shellData = [head.map(item => item)].concat(data);
          this.setHistory(record);
        }
      },
      error(res) {
        let record = {
          createdAt: Date.now(),
          time: res.timing,
          cluster: this.current_cluster.name,
          // database: state.useDB,
          sql: command,
          type: 0,
          rows: 0,
          message: res.message,
          appId: this.current_cluster.id,
        };
        this.setHistory(record);
      },
      getCursorPosition() {
        return term.buffer.active.cursorX - this.promptStr.length;
      },
      write(data) {
        term.write(data, () => {
          serializeAddon.serialize();
        });
      },
      initHistory() {
        this.history.forEach(item => {
          this.writeShell(item);
        });
      },
      writeShell(data, command = true) {
        command && term.writeln(`$ ${data.database ? data.database + "> " : ""}${data.sql}`);
        switch (data.type) {
          case 0:
            term.writeln(data.message);
            break;
          case 1:
            if (!command && this.shellData.length) {
              this.write(table(this.shellData).replace(/\n/g, "\r\n"));
            }
            term.writeln(`Query OK,${data.rows} rows in database (${data.time} ms)`);
            break;
          default:
            break;
        }
        command && term.writeln("");
      },
      writeCustomContent() {
        CustomShellContent.forEach(item => {
          term.writeln(item);
        });
      },
      powerlineSymbolTest(term) {
        function s(char) {
          return `${char} \x1b[7m${char}\x1b[0m  `;
        }
        term.write("\n\n\r");
        term.writeln("Standard powerline symbols:");
        term.writeln("      0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F");
        term.writeln(`0xA_  ${s("\ue0a0")}${s("\ue0a1")}${s("\ue0a2")}`);
        term.writeln(`0xB_  ${s("\ue0b0")}${s("\ue0b1")}${s("\ue0b2")}${s("\ue0b3")}`);
        term.writeln("");
        term.writeln(
          `\x1b[7m` +
            ` inverse \ue0b1 \x1b[0;40m\ue0b0` +
            ` 0 \ue0b1 \x1b[30;41m\ue0b0\x1b[39m` +
            ` 1 \ue0b1 \x1b[31;42m\ue0b0\x1b[39m` +
            ` 2 \ue0b1 \x1b[32;43m\ue0b0\x1b[39m` +
            ` 3 \ue0b1 \x1b[33;44m\ue0b0\x1b[39m` +
            ` 4 \ue0b1 \x1b[34;45m\ue0b0\x1b[39m` +
            ` 5 \ue0b1 \x1b[35;46m\ue0b0\x1b[39m` +
            ` 6 \ue0b1 \x1b[36;47m\ue0b0\x1b[30m` +
            ` 7 \ue0b1 \x1b[37;49m\ue0b0\x1b[0m`
        );
        term.writeln("");
        term.writeln(
          `\x1b[7m` +
            ` inverse \ue0b3 \x1b[0;7;40m\ue0b2\x1b[27m` +
            ` 0 \ue0b3 \x1b[7;30;41m\ue0b2\x1b[27;39m` +
            ` 1 \ue0b3 \x1b[7;31;42m\ue0b2\x1b[27;39m` +
            ` 2 \ue0b3 \x1b[7;32;43m\ue0b2\x1b[27;39m` +
            ` 3 \ue0b3 \x1b[7;33;44m\ue0b2\x1b[27;39m` +
            ` 4 \ue0b3 \x1b[7;34;45m\ue0b2\x1b[27;39m` +
            ` 5 \ue0b3 \x1b[7;35;46m\ue0b2\x1b[27;39m` +
            ` 6 \ue0b3 \x1b[7;36;47m\ue0b2\x1b[27;30m` +
            ` 7 \ue0b3 \x1b[7;37;49m\ue0b2\x1b[0m`
        );
        term.writeln("");
        term.writeln(
          `\x1b[7m` +
            ` inverse \ue0b5 \x1b[0;40m\ue0b4` +
            ` 0 \ue0b5 \x1b[30;41m\ue0b4\x1b[39m` +
            ` 1 \ue0b5 \x1b[31;42m\ue0b4\x1b[39m` +
            ` 2 \ue0b5 \x1b[32;43m\ue0b4\x1b[39m` +
            ` 3 \ue0b5 \x1b[33;44m\ue0b4\x1b[39m` +
            ` 4 \ue0b5 \x1b[34;45m\ue0b4\x1b[39m` +
            ` 5 \ue0b5 \x1b[35;46m\ue0b4\x1b[39m` +
            ` 6 \ue0b5 \x1b[36;47m\ue0b4\x1b[30m` +
            ` 7 \ue0b5 \x1b[37;49m\ue0b4\x1b[0m`
        );
        term.writeln("");
        term.writeln(
          `\x1b[7m` +
            ` inverse \ue0b7 \x1b[0;7;40m\ue0b6\x1b[27m` +
            ` 0 \ue0b7 \x1b[7;30;41m\ue0b6\x1b[27;39m` +
            ` 1 \ue0b7 \x1b[7;31;42m\ue0b6\x1b[27;39m` +
            ` 2 \ue0b7 \x1b[7;32;43m\ue0b6\x1b[27;39m` +
            ` 3 \ue0b7 \x1b[7;33;44m\ue0b6\x1b[27;39m` +
            ` 4 \ue0b7 \x1b[7;34;45m\ue0b6\x1b[27;39m` +
            ` 5 \ue0b7 \x1b[7;35;46m\ue0b6\x1b[27;39m` +
            ` 6 \ue0b7 \x1b[7;36;47m\ue0b6\x1b[27;30m` +
            ` 7 \ue0b7 \x1b[7;37;49m\ue0b6\x1b[0m`
        );
        term.writeln("");
        term.writeln("Powerline extra symbols:");
        term.writeln("      0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F");
        term.writeln(`0xA_                 ${s("\ue0a3")}`);
        term.writeln(
          `0xB_                      ${s("\ue0b4")}${s("\ue0b5")}${s("\ue0b6")}${s("\ue0b7")}${s("\ue0b8")}${s("\ue0b9")}${s("\ue0ba")}${s(
            "\ue0bb"
          )}${s("\ue0bc")}${s("\ue0bd")}${s("\ue0be")}${s("\ue0bf")}`
        );
        term.writeln(
          `0xC_  ${s("\ue0c0")}${s("\ue0c1")}${s("\ue0c2")}${s("\ue0c3")}${s("\ue0c4")}${s("\ue0c5")}${s("\ue0c6")}${s("\ue0c7")}${s("\ue0c8")}${s(
            "\ue0c9"
          )}${s("\ue0ca")}${s("\ue0cb")}${s("\ue0cc")}${s("\ue0cd")}${s("\ue0be")}${s("\ue0bf")}`
        );
        term.writeln(`0xD_  ${s("\ue0d0")}${s("\ue0d1")}${s("\ue0d2")}     ${s("\ue0d4")}`);
        term.writeln("");
        term.writeln("Sample of nerd fonts icons:");
        term.writeln("    nf-linux-apple (\\uF302) \uf302");
        term.writeln("nf-mdi-github_face (\\uFbd9) \ufbd9");
      },
      writeCustomGlyphHandler(term) {
        term.write("\n\r");
        term.write("\n\r");
        term.write("Box styles:       ┎┰┒┍┯┑╓╥╖╒╤╕ ┏┳┓┌┲┓┌┬┐┏┱┐\n\r");
        term.write("┌─┬─┐ ┏━┳━┓ ╔═╦═╗ ┠╂┨┝┿┥╟╫╢╞╪╡ ┡╇┩├╊┫┢╈┪┣╉┤\n\r");
        term.write("│ │ │ ┃ ┃ ┃ ║ ║ ║ ┖┸┚┕┷┙╙╨╜╘╧╛ └┴┘└┺┛┗┻┛┗┹┘\n\r");
        term.write("├─┼─┤ ┣━╋━┫ ╠═╬═╣ ┏┱┐┌┲┓┌┬┐┌┬┐ ┏┳┓┌┮┓┌┬┐┏┭┐\n\r");
        term.write("│ │ │ ┃ ┃ ┃ ║ ║ ║ ┡╃┤├╄┩├╆┪┢╅┤ ┞╀┦├┾┫┟╁┧┣┽┤\n\r");
        term.write("└─┴─┘ ┗━┻━┛ ╚═╩═╝ └┴┘└┴┘└┺┛┗┹┘ └┴┘└┶┛┗┻┛┗┵┘\n\r");
        term.write("\n\r");
        term.write("Other:\n\r");
        term.write("╭─╮ ╲ ╱ ╷╻╎╏┆┇┊┋ ╺╾╴ ╌╌╌ ┄┄┄ ┈┈┈\n\r");
        term.write("│ │  ╳  ╽╿╎╏┆┇┊┋ ╶╼╸ ╍╍╍ ┅┅┅ ┉┉┉\n\r");
        term.write("╰─╯ ╱ ╲ ╹╵╎╏┆┇┊┋\n\r");
        term.write("\n\r");
        term.write("All box drawing characters:\n\r");
        term.write("─ ━ │ ┃ ┄ ┅ ┆ ┇ ┈ ┉ ┊ ┋ ┌ ┍ ┎ ┏\n\r");
        term.write("┐ ┑ ┒ ┓ └ ┕ ┖ ┗ ┘ ┙ ┚ ┛ ├ ┝ ┞ ┟\n\r");
        term.write("┠ ┡ ┢ ┣ ┤ ┥ ┦ ┧ ┨ ┩ ┪ ┫ ┬ ┭ ┮ ┯\n\r");
        term.write("┰ ┱ ┲ ┳ ┴ ┵ ┶ ┷ ┸ ┹ ┺ ┻ ┼ ┽ ┾ ┿\n\r");
        term.write("╀ ╁ ╂ ╃ ╄ ╅ ╆ ╇ ╈ ╉ ╊ ╋ ╌ ╍ ╎ ╏\n\r");
        term.write("═ ║ ╒ ╓ ╔ ╕ ╖ ╗ ╘ ╙ ╚ ╛ ╜ ╝ ╞ ╟\n\r");
        term.write("╠ ╡ ╢ ╣ ╤ ╥ ╦ ╧ ╨ ╩ ╪ ╫ ╬ ╭ ╮ ╯\n\r");
        term.write("╰ ╱ ╲ ╳ ╴ ╵ ╶ ╷ ╸ ╹ ╺ ╻ ╼ ╽ ╾ ╿\n\r");
        term.write("Box drawing alignment tests:\x1b[31m                                          █\n\r");
        term.write("                                                                      ▉\n\r");
        term.write("  ╔══╦══╗  ┌──┬──┐  ╭──┬──╮  ╭──┬──╮  ┏━━┳━━┓  ┎┒┏┑   ╷  ╻ ┏┯┓ ┌┰┐    ▊ ╱╲╱╲╳╳╳\n\r");
        term.write("  ║┌─╨─┐║  │╔═╧═╗│  │╒═╪═╕│  │╓─╁─╖│  ┃┌─╂─┐┃  ┗╃╄┙  ╶┼╴╺╋╸┠┼┨ ┝╋┥    ▋ ╲╱╲╱╳╳╳\n\r");
        term.write("  ║│╲ ╱│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╿ │┃  ┍╅╆┓   ╵  ╹ ┗┷┛ └┸┘    ▌ ╱╲╱╲╳╳╳\n\r");
        term.write("  ╠╡ ╳ ╞╣  ├╢   ╟┤  ├┼─┼─┼┤  ├╫─╂─╫┤  ┣┿╾┼╼┿┫  ┕┛┖┚     ┌┄┄┐ ╎ ┏┅┅┓ ┋ ▍ ╲╱╲╱╳╳╳\n\r");
        term.write("  ║│╱ ╲│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╽ │┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▎\n\r");
        term.write("  ║└─╥─┘║  │╚═╤═╝│  │╘═╪═╛│  │╙─╀─╜│  ┃└─╂─┘┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▏\n\r");
        term.write("  ╚══╩══╝  └──┴──┘  ╰──┴──╯  ╰──┴──╯  ┗━━┻━━┛           └╌╌┘ ╎ ┗╍╍┛ ┋  ▁▂▃▄▅▆▇█\n\r");
        term.write("Box drawing alignment tests:\x1b[32m                                          █\n\r");
        term.write("                                                                      ▉\n\r");
        term.write("  ╔══╦══╗  ┌──┬──┐  ╭──┬──╮  ╭──┬──╮  ┏━━┳━━┓  ┎┒┏┑   ╷  ╻ ┏┯┓ ┌┰┐    ▊ ╱╲╱╲╳╳╳\n\r");
        term.write("  ║┌─╨─┐║  │╔═╧═╗│  │╒═╪═╕│  │╓─╁─╖│  ┃┌─╂─┐┃  ┗╃╄┙  ╶┼╴╺╋╸┠┼┨ ┝╋┥    ▋ ╲╱╲╱╳╳╳\n\r");
        term.write("  ║│╲ ╱│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╿ │┃  ┍╅╆┓   ╵  ╹ ┗┷┛ └┸┘    ▌ ╱╲╱╲╳╳╳\n\r");
        term.write("  ╠╡ ╳ ╞╣  ├╢   ╟┤  ├┼─┼─┼┤  ├╫─╂─╫┤  ┣┿╾┼╼┿┫  ┕┛┖┚     ┌┄┄┐ ╎ ┏┅┅┓ ┋ ▍ ╲╱╲╱╳╳╳\n\r");
        term.write("  ║│╱ ╲│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╽ │┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▎\n\r");
        term.write("  ║└─╥─┘║  │╚═╤═╝│  │╘═╪═╛│  │╙─╀─╜│  ┃└─╂─┘┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▏\n\r");
        term.write("  ╚══╩══╝  └──┴──┘  ╰──┴──╯  ╰──┴──╯  ┗━━┻━━┛           └╌╌┘ ╎ ┗╍╍┛ ┋  ▁▂▃▄▅▆▇█\n\r");
        window.scrollTo(0, 0);
      },
      addSqlVal(val) {
        term.write(val);
        if (this.$store.state.console.partActive == "xterm") {
          this.$store.state.console.addSql = "";
          term.focus();
        }
      },
      setHistory(data) {
        this.currentOutput = data;
        this.history.push(data);
        this.history = this.history.slice(-50);
        localStorage.setItem(this.historyKey, JSON.stringify(this.history));
      },
      copy(data) {
        copy(data, () => {});
      },
      mousedown(e) {
        if (e.button == 2) {
          console.log("dsa");
        }
      },
      contextmenu(e) {
        e.preventDefault();
      },
    },
    beforeDestroy() {
      if (this.ws) {
        this.ws.close();
      }
    },
  };
</script>

<style scoped lang="scss">
  #terminal {
    height: 100%;

    // overflow: auto;
  }
  .terminal-wrapper {
    height: 100%;
    padding: 10px 20px;
    background-color: #282a36;
  }
</style>
