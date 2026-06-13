package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Command string

const (
	CommandRun    Command = "run"
	CommandReplay Command = "replay"
	CommandServe  Command = "serve"
)

const DefaultTargetDSN = "root:taosdata@tcp(127.0.0.1:6030)/"
const DefaultOutDir = "out"

type Parsed struct {
	Command     Command
	Run         RunConfig
	Replay      ReplayConfig
	Serve       ServeConfig
	ShowHelp    bool
	ShowVersion bool
}

type RunConfig struct {
	DSN                  string
	Seed                 int64
	RNGState             string
	Cases                int
	Duration             time.Duration
	StmtTimeout          time.Duration
	OutDir               string
	CleanupSuccessRunDir bool
	MutationLevel        int
	StopWhenCovered      bool
	DryRun               bool
	Verbose              bool
	DumpAllQueries       bool
	DumpAllGraphs        bool
	ExcludeCatalog       bool
	LegacyMode           bool
	WorkloadConfig       string
	ExecProfile          string
}

type ReplayConfig struct {
	DSN         string
	File        string
	Count       int
	StmtTimeout time.Duration
}

type ServeConfig struct {
	Listen      string
	APIToken    string
	DataDir     string
	OutDir      string
	AllowOrigin string
}

var legacyOptRE = regexp.MustCompile(`^--(help|verbose|target|sqlite|monetdb|version|dump-all-graphs|dump-all-queries|seed|dry-run|max-queries|rng-state|exclude-catalog|config|exec-profile)(?:=((?:.|\n)*))?$`)

func Parse(args []string) (*Parsed, error) {
	if len(args) == 0 {
		return nil, usageErr("missing subcommand or classic options")
	}
	if strings.HasPrefix(args[0], "--") {
		return parseLegacy(args)
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		return &Parsed{ShowHelp: true}, nil
	}

	switch args[0] {
	case string(CommandRun):
		runCfg, err := parseRun(args[1:])
		if err != nil {
			return nil, err
		}
		return &Parsed{Command: CommandRun, Run: *runCfg}, nil
	case string(CommandReplay):
		replayCfg, err := parseReplay(args[1:])
		if err != nil {
			return nil, err
		}
		return &Parsed{Command: CommandReplay, Replay: *replayCfg}, nil
	case string(CommandServe):
		serveCfg, err := parseServe(args[1:])
		if err != nil {
			return nil, err
		}
		return &Parsed{Command: CommandServe, Serve: *serveCfg}, nil
	default:
		return nil, usageErr(fmt.Sprintf("unknown subcommand %q", args[0]))
	}
}

func parseLegacy(args []string) (*Parsed, error) {
	opts := map[string]string{}
	for _, raw := range args {
		m := legacyOptRE.FindStringSubmatch(raw)
		if m == nil {
			return nil, usageErr(fmt.Sprintf("cannot parse option: %s", raw))
		}
		opts[m[1]] = m[2]
	}
	if _, ok := opts["help"]; ok {
		return &Parsed{ShowHelp: true}, nil
	}
	if _, ok := opts["version"]; ok {
		return &Parsed{ShowVersion: true}, nil
	}
	if opts["sqlite"] != "" || opts["monetdb"] != "" {
		return nil, usageErr("this implementation supports TDengine only; use --target")
	}

	cfg := RunConfig{
		DSN:                  DefaultTargetDSN,
		Seed:                 int64(os.Getpid()),
		RNGState:             opts["rng-state"],
		Cases:                0,
		StmtTimeout:          2 * time.Second,
		OutDir:               DefaultOutDir,
		CleanupSuccessRunDir: true,
		MutationLevel:        1,
		StopWhenCovered:      false,
		DryRun:               hasOpt(opts, "dry-run"),
		Verbose:              hasOpt(opts, "verbose"),
		DumpAllQueries:       hasOpt(opts, "dump-all-queries"),
		DumpAllGraphs:        hasOpt(opts, "dump-all-graphs"),
		ExcludeCatalog:       hasOpt(opts, "exclude-catalog"),
		LegacyMode:           true,
		WorkloadConfig:       strings.TrimSpace(opts["config"]),
		ExecProfile:          strings.TrimSpace(opts["exec-profile"]),
	}
	if cfg.ExecProfile == "" {
		cfg.ExecProfile = "strict"
	}
	if v := strings.TrimSpace(opts["target"]); v != "" {
		cfg.DSN = v
	}
	if s := strings.TrimSpace(opts["seed"]); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, usageErr(fmt.Sprintf("invalid --seed=%q: %v", s, err))
		}
		cfg.Seed = n
	}
	if mq := strings.TrimSpace(opts["max-queries"]); mq != "" {
		n, err := strconv.ParseInt(mq, 10, 64)
		if err != nil || n < 0 {
			return nil, usageErr(fmt.Sprintf("invalid --max-queries=%q", mq))
		}
		cfg.Cases = int(n)
	}
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --out-dir: %w", err)
	}
	cfg.OutDir = absOut

	return &Parsed{Command: CommandRun, Run: cfg}, nil
}

func hasOpt(m map[string]string, key string) bool {
	_, ok := m[key]
	return ok
}

func parseRun(args []string) (*RunConfig, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(new(strings.Builder))

	cfg := &RunConfig{}
	target := fs.String("target", "", "TDengine DSN (sqlsmith-compatible alias)")
	seed := fs.Int64("seed", time.Now().UnixNano(), "seed")
	maxQueries := fs.Int("max-queries", -1, "sqlsmith-compatible alias for --cases")

	fs.StringVar(&cfg.DSN, "dsn", "", "TDengine DSN")
	fs.IntVar(&cfg.Cases, "cases", 2000, "generated query count")
	duration := fs.Duration("duration", 0, "run duration, e.g. 10m")
	fs.DurationVar(&cfg.StmtTimeout, "stmt-timeout", 2*time.Second, "statement timeout")
	fs.StringVar(&cfg.OutDir, "out-dir", DefaultOutDir, "output directory")
	fs.BoolVar(&cfg.CleanupSuccessRunDir, "cleanup-success-run-dir", true, "cleanup temporary child logs only when run exits cleanly and no core dump is observed")
	fs.IntVar(&cfg.MutationLevel, "mutation-level", 1, "0..3")
	fs.BoolVar(&cfg.StopWhenCovered, "stop-when-covered", true, "stop when all positive branches are covered")
	fs.BoolVar(&cfg.DryRun, "dry-run", false, "parse-gate only, skip TDengine execution")
	fs.BoolVar(&cfg.Verbose, "verbose", false, "verbose progress")
	fs.StringVar(&cfg.RNGState, "rng-state", "", "deserialize RNG state")
	fs.BoolVar(&cfg.DumpAllQueries, "dump-all-queries", false, "print generated queries")
	fs.BoolVar(&cfg.DumpAllGraphs, "dump-all-graphs", false, "dump generated AST graphs")
	fs.BoolVar(&cfg.ExcludeCatalog, "exclude-catalog", false, "reserved compatibility option")
	fs.StringVar(&cfg.WorkloadConfig, "config", "", "workload TOML config path (go-sqlsmith style)")
	fs.StringVar(&cfg.ExecProfile, "exec-profile", "strict", "execution profile: strict|balanced|aggressive")

	if err := fs.Parse(args); err != nil {
		return nil, usageErr(err.Error())
	}
	cfg.Seed = *seed
	cfg.Duration = *duration
	if *maxQueries >= 0 {
		cfg.Cases = *maxQueries
	}
	if strings.TrimSpace(*target) != "" {
		cfg.DSN = strings.TrimSpace(*target)
	}

	if cfg.Cases < 0 {
		return nil, usageErr("--cases must be >= 0")
	}
	if cfg.Cases == 0 && cfg.Duration <= 0 {
		cfg.Cases = 2000
	}
	if cfg.StmtTimeout <= 0 {
		return nil, usageErr("--stmt-timeout must be > 0")
	}
	if cfg.MutationLevel < 0 || cfg.MutationLevel > 3 {
		return nil, usageErr("--mutation-level must be in [0,3]")
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ExecProfile)) {
	case "strict", "balanced", "aggressive":
	default:
		return nil, usageErr("--exec-profile must be one of strict|balanced|aggressive")
	}
	if !cfg.DryRun && strings.TrimSpace(cfg.DSN) == "" {
		cfg.DSN = DefaultTargetDSN
	}

	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --out-dir: %w", err)
	}
	cfg.OutDir = absOut
	return cfg, nil
}

func parseReplay(args []string) (*ReplayConfig, error) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(new(strings.Builder))

	cfg := &ReplayConfig{}
	fs.StringVar(&cfg.DSN, "dsn", DefaultTargetDSN, "TDengine DSN")
	fs.StringVar(&cfg.File, "file", "", "run_report.json path")
	fs.IntVar(&cfg.Count, "count", 1, "execution count")
	fs.DurationVar(&cfg.StmtTimeout, "stmt-timeout", 2*time.Second, "statement timeout")

	if err := fs.Parse(args); err != nil {
		return nil, usageErr(err.Error())
	}
	if strings.TrimSpace(cfg.File) == "" {
		return nil, usageErr("--file is required")
	}
	if cfg.Count <= 0 {
		return nil, usageErr("--count must be > 0")
	}
	if cfg.StmtTimeout <= 0 {
		return nil, usageErr("--stmt-timeout must be > 0")
	}

	absFile, err := filepath.Abs(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("resolve --file: %w", err)
	}
	cfg.File = absFile
	return cfg, nil
}

func parseServe(args []string) (*ServeConfig, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(new(strings.Builder))

	cfg := &ServeConfig{}
	fs.StringVar(&cfg.Listen, "listen", ":8080", "listen address")
	fs.StringVar(&cfg.APIToken, "api-token", envOr("TDSQLSMITH_API_TOKEN", "tdsqlsmith-dev-token"), "bearer token for API auth")
	fs.StringVar(&cfg.DataDir, "data-dir", "data", "state data directory")
	fs.StringVar(&cfg.OutDir, "out-dir", DefaultOutDir, "run output directory")
	fs.StringVar(&cfg.AllowOrigin, "allow-origin", "*", "CORS Access-Control-Allow-Origin value")

	if err := fs.Parse(args); err != nil {
		return nil, usageErr(err.Error())
	}
	if strings.TrimSpace(cfg.Listen) == "" {
		return nil, usageErr("--listen is required")
	}
	if strings.TrimSpace(cfg.APIToken) == "" {
		return nil, usageErr("--api-token is required")
	}
	absData, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --data-dir: %w", err)
	}
	cfg.DataDir = absData
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --out-dir: %w", err)
	}
	cfg.OutDir = absOut
	return cfg, nil
}

func Usage() string {
	return strings.TrimSpace(`
Usage:
  tdsqlsmith run [flags]
  tdsqlsmith replay [flags]
  tdsqlsmith serve [flags]

  # sqlsmith-compatible mode (no subcommand):
  tdsqlsmith --target=... --max-queries=1000 --verbose

Run flags:
  --dsn=...                  TDengine DSN
  --target=...               sqlsmith-compatible alias of --dsn
  --seed=INT                 RNG seed
  --rng-state=HEX            deserialize RNG state
  --cases=INT                generated query count
  --max-queries=INT          sqlsmith-compatible alias for --cases
  --duration=DURATION        run duration, e.g. 10m
  --stmt-timeout=DURATION    single statement timeout
  --out-dir=PATH             output directory
  --cleanup-success-run-dir=true|false (cleanup temp child logs; reports are always kept)
  --mutation-level=0..3      mutation intensity
  --stop-when-covered=true|false
  --dry-run
  --verbose
  --dump-all-queries
  --dump-all-graphs
  --exclude-catalog
  --config=PATH              workload TOML config (go-sqlsmith style)
  --exec-profile=PROFILE     strict|balanced|aggressive

Replay flags:
  --dsn=...
  --file=PATH
  --count=INT
  --stmt-timeout=DURATION

Serve flags:
  --listen=:8080
  --api-token=TOKEN
  --data-dir=PATH
  --out-dir=PATH
  --allow-origin=*
`)
}

func usageErr(msg string) error {
	return errors.New(strings.TrimSpace(msg))
}

func PrintUsage() {
	_, _ = fmt.Fprintln(os.Stderr, Usage())
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
