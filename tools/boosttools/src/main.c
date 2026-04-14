/*
 * main.c -- CLI entry point for boosttools.
 *
 * Usage:
 *   boosttools --src-host <ip> --src-port <port> --src-user <user> --src-pass <pass>
 *              --dst-host <ip> --dst-port <port> --dst-user <user> --dst-pass <pass>
 *              [--database <db>] [--stable <stable>]
 *              [--workers <n>] [--pool-size <n>]
 *              [--schema-only] [--data-only] [--resume]
 *              [--time-start <ts>] [--time-end <ts>]
 *              [--verbose] [--dry-run]
 */

#include "boost.h"

#include <getopt.h>
#include <signal.h>
#include <unistd.h>

/* Global flag for graceful shutdown. */
static volatile sig_atomic_t g_shutdown = 0;

static void signal_handler(int sig)
{
    (void)sig;
    g_shutdown = 1;
    BOOST_LOG("Shutdown signal received, finishing current work...");
}

static void print_usage(const char *prog)
{
    fprintf(stderr,
        "boosttools - High-performance TDengine cluster-to-cluster sync\n"
        "\n"
        "Usage: %s [options]\n"
        "\n"
        "Source cluster:\n"
        "  --src-host <ip>        Source host (default: localhost)\n"
        "  --src-port <port>      Source port (default: 6030)\n"
        "  --src-user <user>      Source user (default: root)\n"
        "  --src-pass <pass>      Source password (default: taosdata)\n"
        "\n"
        "Destination cluster:\n"
        "  --dst-host <ip>        Destination host (default: localhost)\n"
        "  --dst-port <port>      Destination port (default: 6030)\n"
        "  --dst-user <user>      Destination user (default: root)\n"
        "  --dst-pass <pass>      Destination password (default: taosdata)\n"
        "\n"
        "Scope:\n"
        "  --database <db>        Sync only this database (default: all)\n"
        "  --stable <stable>      Sync only this supertable (default: all)\n"
        "  --time-start <ts>      Start timestamp filter (ISO format)\n"
        "  --time-end <ts>        End timestamp filter (ISO format)\n"
        "\n"
        "Performance:\n"
        "  --workers <n>          Number of worker threads (default: 16, max: 64)\n"
        "  --pool-size <n>        Connection pool size (default: workers+2)\n"
        "\n"
        "Mode:\n"
        "  --schema-only          Sync schema only, skip data\n"
        "  --data-only            Sync data only, skip schema\n"
        "  --resume               Resume from last checkpoint\n"
        "  --dry-run              Print what would be done without executing\n"
        "  --verbose              Enable verbose debug logging\n"
        "  --help                 Show this help\n"
        "\n"
        "Examples:\n"
        "  # Full sync between two clusters\n"
        "  %s --src-host 10.0.1.1 --dst-host 10.0.2.1 --database mydb\n"
        "\n"
        "  # Resume interrupted sync with more workers\n"
        "  %s --src-host 10.0.1.1 --dst-host 10.0.2.1 --database mydb \\\n"
        "     --data-only --resume --workers 32\n"
        "\n"
        "  # Sync specific time range\n"
        "  %s --src-host 10.0.1.1 --dst-host 10.0.2.1 --database mydb \\\n"
        "     --time-start '2024-01-01' --time-end '2024-06-01'\n"
        "\n",
        prog, prog, prog, prog);
}

static void print_banner(void)
{
    BOOST_LOG("╔══════════════════════════════════════════════╗");
    BOOST_LOG("║         boosttools v1.0                      ║");
    BOOST_LOG("║  High-performance TDengine Cluster Sync      ║");
    BOOST_LOG("║  Engine: raw_block zero-copy transfer         ║");
    BOOST_LOG("╚══════════════════════════════════════════════╝");
}

static int parse_args(int argc, char *argv[], BoostConfig *cfg)
{
    /* Set defaults. */
    strncpy(cfg->src.host, "localhost", BOOST_MAX_HOST_LEN - 1);
    cfg->src.port = 6030;
    strncpy(cfg->src.user, "root", sizeof(cfg->src.user) - 1);
    strncpy(cfg->src.pass, "taosdata", sizeof(cfg->src.pass) - 1);

    strncpy(cfg->dst.host, "localhost", BOOST_MAX_HOST_LEN - 1);
    cfg->dst.port = 6030;
    strncpy(cfg->dst.user, "root", sizeof(cfg->dst.user) - 1);
    strncpy(cfg->dst.pass, "taosdata", sizeof(cfg->dst.pass) - 1);

    cfg->num_workers = BOOST_DEFAULT_WORKERS;
    cfg->conn_pool_size = 0; /* auto */
    cfg->batch_size = BOOST_DEFAULT_BATCH_SIZE;
    cfg->schema_only = false;
    cfg->data_only = false;
    cfg->resume = false;
    cfg->verbose = false;
    cfg->dry_run = false;
    cfg->database[0] = '\0';
    cfg->stable[0] = '\0';
    cfg->time_start[0] = '\0';
    cfg->time_end[0] = '\0';

    static struct option long_options[] = {
        {"src-host",    required_argument, NULL, 'A'},
        {"src-port",    required_argument, NULL, 'B'},
        {"src-user",    required_argument, NULL, 'C'},
        {"src-pass",    required_argument, NULL, 'D'},
        {"dst-host",    required_argument, NULL, 'E'},
        {"dst-port",    required_argument, NULL, 'F'},
        {"dst-user",    required_argument, NULL, 'G'},
        {"dst-pass",    required_argument, NULL, 'H'},
        {"database",    required_argument, NULL, 'd'},
        {"stable",      required_argument, NULL, 's'},
        {"workers",     required_argument, NULL, 'w'},
        {"pool-size",   required_argument, NULL, 'p'},
        {"time-start",  required_argument, NULL, 'T'},
        {"time-end",    required_argument, NULL, 'U'},
        {"schema-only", no_argument,       NULL, 'S'},
        {"data-only",   no_argument,       NULL, 'O'},
        {"resume",      no_argument,       NULL, 'r'},
        {"verbose",     no_argument,       NULL, 'v'},
        {"dry-run",     no_argument,       NULL, 'n'},
        {"help",        no_argument,       NULL, 'h'},
        {NULL, 0, NULL, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "d:s:w:p:SOrv nh", long_options, NULL)) != -1) {
        switch (opt) {
        case 'A': strncpy(cfg->src.host, optarg, BOOST_MAX_HOST_LEN - 1); break;
        case 'B': cfg->src.port = (uint16_t)atoi(optarg); break;
        case 'C': strncpy(cfg->src.user, optarg, sizeof(cfg->src.user) - 1); break;
        case 'D': strncpy(cfg->src.pass, optarg, sizeof(cfg->src.pass) - 1); break;
        case 'E': strncpy(cfg->dst.host, optarg, BOOST_MAX_HOST_LEN - 1); break;
        case 'F': cfg->dst.port = (uint16_t)atoi(optarg); break;
        case 'G': strncpy(cfg->dst.user, optarg, sizeof(cfg->dst.user) - 1); break;
        case 'H': strncpy(cfg->dst.pass, optarg, sizeof(cfg->dst.pass) - 1); break;
        case 'd': strncpy(cfg->database, optarg, BOOST_MAX_DB_NAME - 1); break;
        case 's': strncpy(cfg->stable, optarg, BOOST_MAX_TB_NAME - 1); break;
        case 'w': cfg->num_workers = atoi(optarg); break;
        case 'p': cfg->conn_pool_size = atoi(optarg); break;
        case 'T': strncpy(cfg->time_start, optarg, sizeof(cfg->time_start) - 1); break;
        case 'U': strncpy(cfg->time_end, optarg, sizeof(cfg->time_end) - 1); break;
        case 'S': cfg->schema_only = true; break;
        case 'O': cfg->data_only = true; break;
        case 'r': cfg->resume = true; break;
        case 'v': cfg->verbose = true; break;
        case 'n': cfg->dry_run = true; break;
        case 'h': print_usage(argv[0]); return -1;
        default:  print_usage(argv[0]); return -1;
        }
    }

    /* Validate. */
    if (cfg->num_workers < 1) cfg->num_workers = 1;
    if (cfg->num_workers > BOOST_MAX_WORKERS) cfg->num_workers = BOOST_MAX_WORKERS;

    if (cfg->conn_pool_size <= 0) {
        cfg->conn_pool_size = cfg->num_workers + 2;
    }
    if (cfg->conn_pool_size > BOOST_MAX_CONNS) {
        cfg->conn_pool_size = BOOST_MAX_CONNS;
    }

    if (cfg->schema_only && cfg->data_only) {
        BOOST_ERR("--schema-only and --data-only are mutually exclusive");
        return -1;
    }

    return 0;
}

static void print_config(const BoostConfig *cfg)
{
    BOOST_LOG("Configuration:");
    BOOST_LOG("  Source:      %s:%u (user=%s)", cfg->src.host, cfg->src.port, cfg->src.user);
    BOOST_LOG("  Destination: %s:%u (user=%s)", cfg->dst.host, cfg->dst.port, cfg->dst.user);
    BOOST_LOG("  Database:    %s", cfg->database[0] ? cfg->database : "(all)");
    BOOST_LOG("  Supertable:  %s", cfg->stable[0] ? cfg->stable : "(all)");
    BOOST_LOG("  Workers:     %d", cfg->num_workers);
    BOOST_LOG("  Pool size:   %d", cfg->conn_pool_size);
    BOOST_LOG("  Mode:        %s",
              cfg->schema_only ? "schema-only" :
              cfg->data_only   ? "data-only"   : "full sync");
    if (cfg->time_start[0]) BOOST_LOG("  Time start:  %s", cfg->time_start);
    if (cfg->time_end[0])   BOOST_LOG("  Time end:    %s", cfg->time_end);
    if (cfg->resume)    BOOST_LOG("  Resume:      yes");
    if (cfg->dry_run)   BOOST_LOG("  Dry run:     yes");
    if (cfg->verbose)   BOOST_LOG("  Verbose:     yes");
}

/* ========================================================================== */
/*  main                                                                      */
/* ========================================================================== */

int main(int argc, char *argv[])
{
    int rc = 0;

    /* Parse arguments. */
    BoostConfig cfg;
    memset(&cfg, 0, sizeof(cfg));
    if (parse_args(argc, argv, &cfg) != 0) {
        return 1;
    }

    print_banner();
    print_config(&cfg);

    /* Install signal handlers. */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Initialize TDengine client library. */
    taos_init();

    /* Initialize progress tracker. */
    BoostProgress progress;
    boost_progress_init(&progress, BOOST_PROGRESS_FILE);

    /* Initialize connection pools. */
    BoostConnPool src_pool, dst_pool;

    BOOST_LOG("Connecting to source cluster %s:%u ...", cfg.src.host, cfg.src.port);
    if (boost_pool_init(&src_pool, &cfg.src, cfg.conn_pool_size) != 0) {
        BOOST_ERR("Failed to initialize source connection pool");
        rc = 1;
        goto cleanup_progress;
    }

    BOOST_LOG("Connecting to destination cluster %s:%u ...", cfg.dst.host, cfg.dst.port);
    if (boost_pool_init(&dst_pool, &cfg.dst, cfg.conn_pool_size) != 0) {
        BOOST_ERR("Failed to initialize destination connection pool");
        rc = 1;
        goto cleanup_src_pool;
    }

    BOOST_LOG("All connections established.");

    /* Phase 1: Schema sync. */
    if (!cfg.data_only) {
        BOOST_LOG("");
        BOOST_LOG("Phase 1: Schema Synchronization");
        rc = boost_schema_sync(&src_pool, &dst_pool, &cfg, &progress);
        if (rc != 0) {
            BOOST_ERR("Schema sync failed");
            goto cleanup_pools;
        }
    }

    /* Phase 2: Data sync. */
    if (!cfg.schema_only && !g_shutdown) {
        BOOST_LOG("");
        BOOST_LOG("Phase 2: Data Transfer (raw_block engine)");
        rc = boost_data_sync(&src_pool, &dst_pool, &cfg, &progress);
        if (rc != 0) {
            BOOST_ERR("Data sync failed");
            goto cleanup_pools;
        }
    }

    /* Final summary. */
    BOOST_LOG("");
    BOOST_LOG("╔══════════════════════════════════════════════╗");
    BOOST_LOG("║              Sync Complete                    ║");
    BOOST_LOG("╚══════════════════════════════════════════════╝");
    boost_progress_print(&progress);

cleanup_pools:
    boost_pool_destroy(&dst_pool);
cleanup_src_pool:
    boost_pool_destroy(&src_pool);
cleanup_progress:
    boost_progress_destroy(&progress);

    taos_cleanup();

    return rc;
}
