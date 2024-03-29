/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _BSD_SOURCE
#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "shellInt.h"

bool shellRegexMatch(const char *s, const char *reg, int32_t cflags) {
  regex_t regex = {0};
  char    msgbuf[100] = {0};

  /* Compile regular expression */
  if (regcomp(&regex, reg, cflags) != 0) {
    fprintf(stderr, "Fail to compile regex");
    shellExit();
  }

  /* Execute regular expression */
  int32_t reti = regexec(&regex, s, 0, NULL, 0);
  if (!reti) {
    regfree(&regex);
    return true;
  } else if (reti == REG_NOMATCH) {
    regfree(&regex);
    return false;
  } else {
    regerror(reti, &regex, msgbuf, sizeof(msgbuf));
    fprintf(stderr, "Regex match failed: %s\r\n", msgbuf);
    regfree(&regex);
    shellExit();
  }

  return false;
}

int32_t shellCheckIntSize() {
  if (sizeof(int8_t) != 1) {
    printf("int8 size is %d(!= 1)", (int)sizeof(int8_t));
    return -1;
  }
  if (sizeof(int16_t) != 2) {
    printf("int16 size is %d(!= 2)", (int)sizeof(int16_t));
    return -1;
  }
  if (sizeof(int32_t) != 4) {
    printf("int32 size is %d(!= 4)", (int)sizeof(int32_t));
    return -1;
  }
  if (sizeof(int64_t) != 8) {
    printf("int64 size is %d(!= 8)", (int)sizeof(int64_t));
    return -1;
  }
  return 0;
}

void shellPrintVersion() { printf("%s\r\n", shell.info.programVersion); }

void shellGenerateAuth() {
  char secretEncrypt[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)shell.args.password, strlen(shell.args.password), secretEncrypt);
  printf("%s\r\n", secretEncrypt);
  fflush(stdout);
}

void shellDumpConfig() {
  SConfig *pCfg = taosGetCfg();
  if (pCfg == NULL) {
    printf("read global config failed!\r\n");
  } else {
    cfgDumpCfg(pCfg, 1, true);
  }
  fflush(stdout);
}

void shellCheckServerStatus() {
  TSDB_SERVER_STATUS code;

  do {
    char details[1024] = {0};
    code = taos_check_server_status(shell.args.host, shell.args.port, details, 1024);
    switch (code) {
      case TSDB_SRV_STATUS_UNAVAILABLE:
        printf("0: unavailable\r\n");
        break;
      case TSDB_SRV_STATUS_NETWORK_OK:
        printf("1: network ok\r\n");
        break;
      case TSDB_SRV_STATUS_SERVICE_OK:
        printf("2: service ok\r\n");
        break;
      case TSDB_SRV_STATUS_SERVICE_DEGRADED:
        printf("3: service degraded\r\n");
        break;
      case TSDB_SRV_STATUS_EXTING:
        printf("4: exiting\r\n");
        break;
    }
    if (strlen(details) != 0) {
      printf("%s\r\n\r\n", details);
    }
    fflush(stdout);
    if (code == TSDB_SRV_STATUS_NETWORK_OK && shell.args.is_startup) {
      taosMsleep(1000);
    } else {
      break;
    }
  } while (1);
}
#ifdef WEBSOCKET
void shellCheckConnectMode() {
	if (shell.args.dsn) {
		shell.args.cloud = true;
		shell.args.restful = false;
		return;
	}
	if (shell.args.cloud) {
		shell.args.dsn = getenv("TDENGINE_CLOUD_DSN");
		if (shell.args.dsn && strlen(shell.args.dsn) > 4) {
			shell.args.cloud = true;
      shell.args.local = false;
			shell.args.restful = false;
			return;
		}

    shell.args.dsn = getenv("TDENGINE_DSN");
		if (shell.args.dsn && strlen(shell.args.dsn) > 4) {
			shell.args.cloud = true;
      shell.args.local = true;
			shell.args.restful = false;
			return;
		}

		if (shell.args.restful) {
			if (!shell.args.host) {
				shell.args.host = "localhost";
			}
			if (!shell.args.port) {
				shell.args.port = 6041;
			}
			shell.args.dsn = taosMemoryCalloc(1, 1024);
			snprintf(shell.args.dsn, 1024, "ws://%s:%d",
					shell.args.host, shell.args.port);
		}
		shell.args.cloud = false;
		return;
	}
}
#endif

void shellExit() {
  if (shell.conn != NULL) {
    taos_close(shell.conn);
    shell.conn = NULL;
  }
  shell.exit = true;
  taos_cleanup();
}
