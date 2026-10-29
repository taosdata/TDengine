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

/*
 * taosreplay -- workload recording inspector and replayer (DS 6.17-6.24).
 *
 * A development tool, not a shipped product component: it is built from source by
 * whoever needs it and is deliberately absent from the install package (FS 4.10).
 *
 * It uses only public taos.h to talk to the target cluster, and only the shared wlr
 * format library to read recordings. That is the point of the split -- if replay
 * needed client internals, the recording format would not really be the contract
 * between recorder and replayer (DS 7.2).
 *
 * Exit codes (FS 4.12) are part of the interface, since operators script around
 * them:
 *   0  completed, no execution errors
 *   1  reserved for an uncategorized launcher/runtime failure
 *   2  input file, format, or argument error
 *   3  credentials missing or connection failed
 *   4  completed, but with API errors, skips, or incomplete input
 *   5  internal error
 */

#include "replayInt.h"
#include "tcrc32c.h"

static void replayPrintUsage(void) {
  (void)printf(
      "taosreplay -- TDengine workload recording inspector and replayer\n"
      "\n"
      "Usage:\n"
      "  taosreplay inspect --input <path> [--input <path>...] [--verbose]\n"
      "  taosreplay replay  --input <path> [--input <path>...]\n"
      "                     --user-password <user=password> [--user-password ...]\n"
      "                     [--target-host <host> --target-port <port> |\n"
      "                      --endpoint-map <old-host[:port]=new-host[:port]>...]\n"
      "                     [--allow-incomplete] [--unfinished-timeout <seconds>]\n"
      "                     [--report-dir <path>]\n"
      "\n"
      "Notes:\n"
      "  --input accepts a session directory or a single .wlr segment.\n"
      "  Credentials may instead be supplied as TAOSREPLAY_USER_PASSWORD=user=password.\n"
      "  --target-host/--target-port and --endpoint-map are mutually exclusive.\n"
      "  --unfinished-timeout defaults to 300 seconds; use 0 to disable the bound.\n"
      "  A session with a real event gap is refused unless --allow-incomplete is\n"
      "  given. A session that merely has unfinished calls is NOT such a case: a\n"
      "  BEGIN without an END is how a hung call is recorded, and it replays normally.\n"
      "\n"
      "Exit codes: 0 ok, 1 reserved, 2 input/args, 3 credentials/connection, 4 completed with\n"
      "errors or skips, 5 internal error.\n");
}

typedef enum {
  REPLAY_OPT_INPUT = 0,
  REPLAY_OPT_TARGET_HOST,
  REPLAY_OPT_TARGET_PORT,
  REPLAY_OPT_ENDPOINT_MAP,
  REPLAY_OPT_USER_PASSWORD,
  REPLAY_OPT_ALLOW_INCOMPLETE,
  REPLAY_OPT_UNFINISHED_TIMEOUT,
  REPLAY_OPT_REPORT_DIR,
  REPLAY_OPT_VERBOSE,
} EReplayOptionId;

typedef struct {
  const char     *name;
  EReplayOptionId id;
  bool            takesValue;
  bool            replayOnly;
} SReplayOptionSpec;

static const SReplayOptionSpec kReplayOptionSpecs[] = {
    {"--input", REPLAY_OPT_INPUT, true, false},
    {"--target-host", REPLAY_OPT_TARGET_HOST, true, true},
    {"--target-port", REPLAY_OPT_TARGET_PORT, true, true},
    {"--endpoint-map", REPLAY_OPT_ENDPOINT_MAP, true, true},
    {"--user-password", REPLAY_OPT_USER_PASSWORD, true, true},
    {"--allow-incomplete", REPLAY_OPT_ALLOW_INCOMPLETE, false, true},
    {"--unfinished-timeout", REPLAY_OPT_UNFINISHED_TIMEOUT, true, true},
    {"--report-dir", REPLAY_OPT_REPORT_DIR, true, true},
    {"--verbose", REPLAY_OPT_VERBOSE, false, false},
};

static const SReplayOptionSpec *replayFindOptionSpec(const char *name) {
  int32_t i;
  for (i = 0; i < (int32_t)(sizeof(kReplayOptionSpecs) / sizeof(kReplayOptionSpecs[0])); ++i) {
    if (strcmp(name, kReplayOptionSpecs[i].name) == 0) return &kReplayOptionSpecs[i];
  }
  return NULL;
}

static bool replayIsDecimal(const char *value) {
  if (value == NULL) return false;
  const unsigned char *p = (const unsigned char *)value;
  if (*p == 0) return false;
  if (strlen(value) > 10) return false;
  while (*p != 0) {
    if (*p < '0' || *p > '9') return false;
    ++p;
  }
  return true;
}

static bool replayIsUnsignedDecimalSyntax(const char *value) {
  if (value == NULL || value[0] == '\0') return false;
  for (const unsigned char *p = (const unsigned char *)value; *p != 0; ++p) {
    if (*p < '0' || *p > '9') return false;
  }
  return true;
}

static bool replayValidEndpointPart(const char *start, size_t length) {
  size_t hostLen = length;
  const char *portStart = NULL;
  char portText[16];
  int32_t port = 0;
  size_t i;

  if (start == NULL || length == 0) return false;
  if (start[0] == '[') {
    const char *close = (const char *)memchr(start, ']', length);
    if (close == NULL || close == start + 1) return false;
    for (i = 1; i < (size_t)(close - start); ++i) {
      if (start[i] == ' ' || start[i] == '\t' || start[i] == '\r' || start[i] == '\n') return false;
    }
    hostLen = (size_t)(close - start + 1);
    if (hostLen < length) {
      if (start[hostLen] != ':') return false;
      portStart = start + hostLen + 1;
    }
  } else {
    size_t colonCount = 0;
    size_t colon = 0;
    for (i = 0; i < length; ++i) {
      if (start[i] == ':') {
        colonCount++;
        colon = i;
      }
      if (start[i] == ' ' || start[i] == '\t' || start[i] == '\r' || start[i] == '\n') return false;
    }
    if (colonCount > 1) return true;
    if (colonCount == 1) {
      if (colon == 0 || colon + 1 == length) return false;
      hostLen = colon;
      portStart = start + colon + 1;
    }
  }
  if (hostLen == 0) return false;
  if (portStart == NULL) return true;
  size_t portLen = (size_t)(start + length - portStart);
  if (portLen >= sizeof(portText)) return false;
  memcpy(portText, portStart, portLen);
  portText[portLen] = '\0';
  return replayIsDecimal(portText) && taosStr2int32(portText, &port) == 0 && port > 0 && port <= 65535;
}

static bool replayValidEndpointMapping(const char *value) {
  const char *eq = strchr(value, '=');
  if (eq == NULL || eq == value || eq[1] == '\0') return false;
  if (strchr(eq + 1, '=') != NULL) return false;
  return replayValidEndpointPart(value, (size_t)(eq - value)) &&
         replayValidEndpointPart(eq + 1, strlen(eq + 1));
}

static int32_t replayParseArgs(int argc, char **argv, SReplayOpts *opts) {
  bool credentialsOnCommandLine = false;
  memset(opts, 0, sizeof(*opts));
  opts->unfinishedTimeoutSec = 300; /* DS 6.19.1 default */
  opts->targetPort = 0;

  if (argc < 2) {
    replayPrintUsage();
    return REPLAY_EXIT_INPUT;
  }

  if (strcmp(argv[1], "inspect") == 0) {
    opts->mode = REPLAY_MODE_INSPECT;
  } else if (strcmp(argv[1], "replay") == 0) {
    opts->mode = REPLAY_MODE_REPLAY;
  } else if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
    replayPrintUsage();
    return REPLAY_EXIT_OK;
  } else {
    (void)fprintf(stderr, "unknown command: %s\n\n", argv[1]);
    replayPrintUsage();
    return REPLAY_EXIT_INPUT;
  }

  for (int i = 2; i < argc; ++i) {
    const char *a = argv[i];
    if (strcmp(a, "--help") == 0 || strcmp(a, "-h") == 0) {
      /* Help is terminal regardless of its position. Keep the already parsed mode
       * only as an internal sentinel so main skips input loading and execution. */
      opts->mode = REPLAY_MODE_NONE;
      replayPrintUsage();
      return REPLAY_EXIT_OK;
    }
    const SReplayOptionSpec *spec = replayFindOptionSpec(a);
    if (spec == NULL) {
      (void)fprintf(stderr, "unknown or incomplete argument: %s\n", a);
      return REPLAY_EXIT_INPUT;
    }
    if (opts->mode == REPLAY_MODE_INSPECT && spec->replayOnly) {
      (void)fprintf(stderr, "%s is only valid with replay\n", a);
      return REPLAY_EXIT_INPUT;
    }
    const char *value = NULL;
    if (spec->takesValue) {
      const char *next = (i + 1 < argc) ? argv[i + 1] : NULL;
      bool nextIsOption = next != NULL &&
                          (replayFindOptionSpec(next) != NULL || strcmp(next, "--help") == 0 ||
                           strcmp(next, "-h") == 0);
      if (next == NULL || next[0] == '\0' || nextIsOption) {
        (void)fprintf(stderr, "missing value for %s\n", a);
        return REPLAY_EXIT_INPUT;
      }
      value = argv[++i];
    }

    switch (spec->id) {
      case REPLAY_OPT_INPUT:
        if (opts->inputCount >= REPLAY_MAX_INPUTS) {
          (void)fprintf(stderr, "too many --input arguments (max %d)\n", REPLAY_MAX_INPUTS);
          return REPLAY_EXIT_INPUT;
        }
        opts->inputs[opts->inputCount++] = value;
        break;
      case REPLAY_OPT_TARGET_HOST:
        opts->targetHost = value;
        break;
      case REPLAY_OPT_TARGET_PORT: {
        /* Range-checked rather than cast: a bare (uint16_t) cast turns 70000 into 4464
         * or a negative into a high port. This tool refuses to connect to an endpoint
         * it cannot resolve exactly, so silently rewriting the port would misdirect
         * replay. */
        int32_t port = 0;
        if (!replayIsDecimal(value) || taosStr2int32(value, &port) != 0 || port <= 0 ||
            port > 65535) {
          (void)fprintf(stderr, "invalid --target-port '%s': expected 1-65535\n", value);
          return REPLAY_EXIT_INPUT;
        }
        opts->targetPort = (uint16_t)port;
        break;
      }
      case REPLAY_OPT_ENDPOINT_MAP: {
        if (opts->endpointMapCount >= REPLAY_MAX_MAPS) {
          (void)fprintf(stderr, "too many --endpoint-map arguments\n");
          return REPLAY_EXIT_INPUT;
        }
        if (!replayValidEndpointMapping(value)) {
          (void)fprintf(stderr,
                        "invalid --endpoint-map '%s': expected <old-host[:port]=new-host[:port]>\n",
                        value);
          return REPLAY_EXIT_INPUT;
        }
        opts->endpointMaps[opts->endpointMapCount++] = value;
        break;
      }
      case REPLAY_OPT_USER_PASSWORD: {
        if (opts->credCount >= REPLAY_MAX_CREDS) {
          (void)fprintf(stderr, "too many --user-password arguments\n");
          return REPLAY_EXIT_INPUT;
        }
        const char *eq = strchr(value, '=');
        if (eq == NULL || eq == value || eq[1] == '\0') {
          (void)fprintf(stderr, "invalid --user-password '%s': expected <user=password>\n", value);
          return REPLAY_EXIT_INPUT;
        }
        opts->creds[opts->credCount++] = value;
        credentialsOnCommandLine = true;
        break;
      }
      case REPLAY_OPT_ALLOW_INCOMPLETE:
        opts->allowIncomplete = true;
        break;
      case REPLAY_OPT_UNFINISHED_TIMEOUT: {
        /* Validated like --target-port rather than trusting atoi: "abc" and "" both
         * yield 0, which is a MEANINGFUL value here ("no timeout"), so a typo would
         * silently disable the bound instead of being rejected. */
        int32_t timeoutSec = 0;
        if (!replayIsUnsignedDecimalSyntax(value)) {
          (void)fprintf(stderr,
                        "invalid --unfinished-timeout '%s': expected decimal seconds (0 = no timeout)\n",
                        value);
          return REPLAY_EXIT_INPUT;
        }
        if (taosStr2int32(value, &timeoutSec) != 0 || timeoutSec < 0) {
          (void)fprintf(stderr,
                        "invalid --unfinished-timeout '%s': value is outside 0..%d seconds\n",
                        value, INT32_MAX);
          return REPLAY_EXIT_INPUT;
        }
        opts->unfinishedTimeoutSec = timeoutSec;
        break;
      }
      case REPLAY_OPT_REPORT_DIR:
        if (!taosIsDir(value)) {
          (void)fprintf(stderr, "--report-dir '%s' is not an existing directory\n", value);
          return REPLAY_EXIT_INPUT;
        }
        opts->reportDir = value;
        break;
      case REPLAY_OPT_VERBOSE:
        opts->verbose = true;
        break;
    }
  }

  if (opts->inputCount == 0) {
    (void)fprintf(stderr, "at least one --input is required\n");
    return REPLAY_EXIT_INPUT;
  }

  if (opts->targetPort != 0 && opts->targetHost == NULL) {
    (void)fprintf(stderr, "--target-port requires --target-host\n");
    return REPLAY_EXIT_INPUT;
  }

  /*
   * Mutually exclusive by design (DS 6.20): mixing a blanket redirect with
   * selective mapping leaves it ambiguous which wins, and the failure mode would be
   * some connections pointing at the recorded (possibly production) endpoint.
   */
  if ((opts->targetHost != NULL || opts->targetPort != 0) && opts->endpointMapCount > 0) {
    (void)fprintf(stderr, "--target-host/--target-port and --endpoint-map are mutually exclusive\n");
    return REPLAY_EXIT_INPUT;
  }

  if (opts->mode == REPLAY_MODE_REPLAY) {
    if (opts->credCount == 0) {
      const char *envCred = getenv("TAOSREPLAY_USER_PASSWORD");
      if (envCred != NULL) {
        const char *eq = strchr(envCred, '=');
        if (eq != NULL && eq != envCred && eq[1] != '\0') {
          opts->creds[opts->credCount++] = envCred;
        } else {
          (void)fprintf(stderr,
                        "invalid TAOSREPLAY_USER_PASSWORD: expected <user=password>\n");
          return REPLAY_EXIT_INPUT;
        }
      }
    }
    if (opts->credCount == 0) {
      /*
       * Refused up front rather than at the first connection: a partial replay that
       * dies halfway has already mutated the target, which is worse than not
       * starting (DS 6.20).
       */
      (void)fprintf(stderr, "--user-password is required for replay\n");
      return REPLAY_EXIT_INPUT;
    }
    if (credentialsOnCommandLine) {
      /* A password supplied on the command line is visible in the process list and
       * shell history. Environment-provided credentials do not trigger this argv-
       * specific warning. */
      (void)fprintf(stderr,
                    "WARNING: --user-password exposes the password in the process list and shell\n"
                    "         history. Use an isolated test cluster; treat the credential as leaked.\n");
    }
  }

  return REPLAY_EXIT_OK;
}

int main(int argc, char **argv) {
  SReplayOpts   opts;
  SReplaySession session;
  int32_t       code;

  code = replayParseArgs(argc, argv, &opts);
  if (code != REPLAY_EXIT_OK) return code;
  if (opts.mode == REPLAY_MODE_NONE) return REPLAY_EXIT_OK; /* --help */

  taosResolveCRC();

  memset(&session, 0, sizeof(session));
  code = replayLoadInputs(&opts, &session);
  if (code != REPLAY_EXIT_OK) {
    replayFreeSession(&session);
    return code;
  }

  if (opts.mode == REPLAY_MODE_INSPECT) {
    bool complete = session.structurallyComplete && !session.sawFatal;
    replayPrintInspect(&opts, &session);
    replayFreeSession(&session);
    return complete ? REPLAY_EXIT_OK : REPLAY_EXIT_PARTIAL;
  }

  code = replayExecute(&opts, &session);
  replayFreeSession(&session);
  return code;
}
