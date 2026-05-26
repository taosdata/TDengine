---
name: tsdb-perf-issue-reproduce
description: "Create a structured workspace for reproducing and analyzing performance issues. Each issue gets its own directory with reproduction scripts, analysis tools, and documentation. Keywords: performance issue, reproduce, workspace, benchmark"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Issue Reproduction

## Quick Start

This skill helps you create a complete workspace for reproducing and analyzing performance issues.

## Workspace Structure

All performance issues are managed under `/root/ccdocs/`, with each issue in its own directory:

```
/root/ccdocs/
├── issue_name_1/
│   ├── README.md                  # Issue overview
│   ├── reproduce_issue.sh         # Reproduction script
│   ├── analyze_performance.sh     # Analysis script
│   ├── notes.md                   # Analysis notes
│   └── results/                   # Test results
│       ├── perf_results.csv
│       ├── perf_report.txt
│       └── flame_graph.svg
├── issue_name_2/
│   └── ...
└── ...
```

## Creating a New Issue Workspace

### Step 1: Create Directory Structure

```bash
# Create workspace for a new issue
ISSUE_NAME="your_issue_name"
mkdir -p /root/ccdocs/${ISSUE_NAME}/results

cd /root/ccdocs/${ISSUE_NAME}
```

### Step 2: Create README.md

```bash
cat > README.md << 'EOF'
# Issue Name

## Problem Description

[Describe the performance issue]

## JIRA Information

- **Issue ID**: XXX-1234
- **Type**: Performance Issue
- **Scenario**: [High concurrency / Large data / etc.]
- **Symptom**: [QPS drops / High latency / etc.]

## Directory Structure

```
/root/ccdocs/issue_name/
├── README.md
├── reproduce_issue.sh
├── analyze_performance.sh
├── notes.md
└── results/
```

## Quick Start

### 1. Reproduce Issue
```bash
./reproduce_issue.sh
```

### 2. Analyze Performance
```bash
./analyze_performance.sh
```

### 3. View Results
```bash
cat results/test_summary.txt
cat results/perf_report.txt
```

## Expected Behavior

[Describe what you expect to see if the issue exists]

## Next Steps

- Review notes.md for detailed analysis
- Use AI to analyze perf reports
- Locate problematic code
- Implement fixes
EOF
```

### Step 3: Create Reproduction Script

```bash
cat > reproduce_issue.sh << 'EOF'
#!/bin/bash

# ============================================
# Issue Reproduction Script
# ============================================

set -e

WORK_DIR="/root/ccdocs/$(basename $(pwd))"
RESULTS_DIR="${WORK_DIR}/results"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# Check taosd
check_taosd() {
    if ! pgrep -x "taosd" > /dev/null; then
        log_warn "taosd is not running!"
        exit 1
    fi
    log_info "taosd is running (PID: $(pgrep taosd))"
}

# Setup environment
setup_env() {
    log_step "Setting up environment..."
    mkdir -p ${RESULTS_DIR}

    # TODO: Add your setup logic here
    # - Create database
    # - Create tables
    # - Insert initial data
}

# Run test
run_test() {
    log_step "Running test..."

    # TODO: Add your test logic here
    # - Start workload
    # - Measure performance
    # - Collect metrics
}

# Generate report
generate_report() {
    log_step "Generating report..."

    cat > ${RESULTS_DIR}/test_summary.txt << SUMMARY_EOF
========================================
Test Summary
========================================

Test Time: $(date)

[Add your test results here]

========================================
SUMMARY_EOF

    cat ${RESULTS_DIR}/test_summary.txt
}

# Main
main() {
    echo "=========================================="
    echo "Issue Reproduction"
    echo "=========================================="

    check_taosd
    setup_env
    run_test
    generate_report

    log_info "Test completed! Results in: ${RESULTS_DIR}/"
}

main
EOF

chmod +x reproduce_issue.sh
```

### Step 4: Create Analysis Script

```bash
cat > analyze_performance.sh << 'EOF'
#!/bin/bash

# ============================================
# Performance Analysis Script
# ============================================

set -e

WORK_DIR="/root/ccdocs/$(basename $(pwd))"
RESULTS_DIR="${WORK_DIR}/results"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

check_taosd() {
    if ! pgrep -x "taosd" > /dev/null; then
        log_warn "taosd is not running!"
        exit 1
    fi
    TAOSD_PID=$(pgrep taosd)
    log_info "taosd PID: ${TAOSD_PID}"
}

analyze_cpu() {
    log_step "CPU Performance Analysis..."

    log_info "Recording CPU profile (30s)..."
    perf record -F 99 -p ${TAOSD_PID} -g -o ${RESULTS_DIR}/perf.data -- sleep 30

    log_info "Generating report..."
    perf report -i ${RESULTS_DIR}/perf.data -n --stdio > ${RESULTS_DIR}/perf_report.txt

    log_info "CPU analysis completed"
}

generate_flamegraph() {
    log_step "Generating flame graph..."

    if [ ! -d "/tmp/FlameGraph" ]; then
        log_info "Cloning FlameGraph..."
        git clone https://github.com/brendangregg/FlameGraph.git /tmp/FlameGraph
    fi

    perf script -i ${RESULTS_DIR}/perf.data | \
        /tmp/FlameGraph/stackcollapse-perf.pl | \
        /tmp/FlameGraph/flamegraph.pl > ${RESULTS_DIR}/flame_graph.svg

    log_info "Flame graph: ${RESULTS_DIR}/flame_graph.svg"
}

main() {
    echo "=========================================="
    echo "Performance Analysis"
    echo "=========================================="

    mkdir -p ${RESULTS_DIR}
    check_taosd

    echo ""
    echo "Options:"
    echo "1. CPU Analysis"
    echo "2. Generate Flame Graph"
    echo "3. Full Analysis"
    echo ""
    read -p "Select (1-3): " choice

    case $choice in
        1) analyze_cpu ;;
        2) generate_flamegraph ;;
        3) analyze_cpu && generate_flamegraph ;;
        *) echo "Invalid choice"; exit 1 ;;
    esac

    log_info "Analysis completed!"
}

main
EOF

chmod +x analyze_performance.sh
```

### Step 5: Create Notes Template

```bash
cat > notes.md << 'EOF'
# Issue Analysis Notes

## Background

[Describe the issue background]

## Test Environment

- Database:
- Tables:
- Data volume:

## Test Scenario

### Workload
- Operation:
- Concurrency:
- Duration:

## Expected vs Actual

### Expected
- [What should happen]

### Actual
- [What actually happens]

## Analysis Steps

### 1. Reproduce Issue
```bash
./reproduce_issue.sh
```

### 2. Performance Analysis
```bash
./analyze_performance.sh
```

### 3. Review Data
- Check results/perf_results.csv
- Review results/perf_report.txt
- View results/flame_graph.svg

### 4. AI Analysis

Provide perf_report.txt to AI and ask:
- What are the main bottlenecks?
- Is there lock contention?
- What functions consume most CPU?
- What are the optimization suggestions?

## Root Cause Analysis

### Hypothesis 1: [Name]
**Description**:

**Verification**:

**Solution**:

### Hypothesis 2: [Name]
**Description**:

**Verification**:

**Solution**:

## Code Location

```bash
# Search for relevant code
grep -rn "function_name" /root/workspace/TDinternal/source/
```

Key files:
-
-

## Optimization Plan

### Option 1: [Name]
- **Description**:
- **Expected improvement**:
- **Implementation difficulty**:
- **Risk**:

### Option 2: [Name]
- **Description**:
- **Expected improvement**:
- **Implementation difficulty**:
- **Risk**:

## Implementation

[Document the actual changes made]

## Verification

[Document how to verify the fix]

## TODO

- [ ] Reproduce issue
- [ ] Collect performance data
- [ ] AI analysis
- [ ] Locate code
- [ ] Design solution
- [ ] Implement fix
- [ ] Verify improvement
- [ ] Submit PR

## Update Log

- YYYY-MM-DD: Created workspace
EOF
```

## Example: Last Cache Issue

See `/root/ccdocs/last_cache_perf_issue/` for a complete example.

## Workflow

### 1. Create Workspace

```bash
cd /root/ccdocs
mkdir -p my_perf_issue/results
cd my_perf_issue
```

### 2. Customize Scripts

Edit the template scripts to match your specific issue:
- Database setup
- Workload generation
- Metrics collection

### 3. Run Reproduction

```bash
./reproduce_issue.sh
```

### 4. Analyze Performance

```bash
./analyze_performance.sh
```

### 5. Use AI for Analysis

```bash
# Copy perf report for AI analysis
cat results/perf_report.txt
```

Ask AI:
- Analyze this perf report
- Identify bottlenecks
- Suggest optimizations

### 6. Document Findings

Update `notes.md` with:
- Root cause analysis
- Optimization plans
- Implementation details

### 7. Implement Fix

```bash
cd /root/workspace/TDinternal
# Make code changes
```

### 8. Verify Fix

```bash
cd /root/ccdocs/my_perf_issue
./reproduce_issue.sh  # Re-run test
# Compare results before/after
```

## Best Practices

### Naming Convention

Use descriptive names for issue directories:
- `last_cache_perf_issue`
- `query_slow_large_dataset`
- `write_lock_contention`
- `memory_leak_continuous_query`

### Documentation

Always document:
- Problem description
- Reproduction steps
- Analysis findings
- Optimization approach
- Verification results

### Version Control

Consider tracking your analysis:
```bash
cd /root/ccdocs/my_perf_issue
git init
git add .
git commit -m "Initial reproduction setup"
```

### Results Archiving

Before re-running tests, archive previous results:
```bash
mv results results_$(date +%Y%m%d_%H%M%S)
mkdir results
```

## Execution Rules

- Create one directory per performance issue
- Use descriptive names
- Include all necessary scripts
- Document findings in notes.md
- Archive results before re-testing
- Use AI to analyze perf reports
- Verify fixes with same test scenario

## Output

This skill produces:
- Structured workspace under /root/ccdocs/
- Reproduction script
- Analysis script
- Documentation templates
- Results directory

## Next Steps

After creating the workspace:
1. Customize scripts for your specific issue
2. Run reproduction to confirm the issue
3. Use analyze_performance.sh to collect data
4. Use AI to analyze the data
5. Follow the optimization workflow
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-issue-reproduce version=0.1.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

