#!/bin/bash

# Extract version and IP from the first two arguments
version="$1"
ip="$2"
shift 2  # Remove the first two arguments, leaving only file paths

# All remaining arguments are considered as file paths
file_paths="$@"

# Execute the awk script and capture the output
readarray -t output < <(awk -v version="$version" -v ip="$ip" '
BEGIN {
    RS = "\\n";  # Set the record separator to newline
    FS = ",";    # Set the field separator to comma
    total = 0;   # Initialize total count
    version_regex = version;  # Use the passed version pattern
    ip_regex = ip;  # Use the passed IP pattern
}
{
    start_collecting = 0;
    version_matched = 0;
    ip_excluded = 0;

    # Check each field within a record
    for (i = 1; i <= NF; i++) {
        if ($i ~ /"ip":"[^"]*"/ && $i ~ ip_regex) {
            ip_excluded = 1;
        }
        if ($i ~ /"version":"[^"]*"/ && $i ~ version_regex) {
            version_matched = 1;
        }
    }

    if (!ip_excluded && version_matched) {
        for (i = 1; i <= NF; i++) {
            if ($i ~ /taosAssertDebug/ && start_collecting == 0) {
                start_collecting = 1;
                continue;
            }
            if (start_collecting == 1 && $i ~ /taosd\(([^)]+)\)/) {
                match($i, /taosd\(([^)]+)\)/, arr);
                if (arr[1] != "") {
                    count[arr[1]]++;
                    total++;
                    break;
                }
            }
        }
    }
}
END {
    for (c in count) {
        printf "%d %s\n", count[c], c;
    }
    print "Total count:", total;
}' $file_paths)

# Capture the function details and total count into separate variables
function_details=$(printf "%s\n" "${output[@]::${#output[@]}-1}")
total_count="${output[-1]}"

# Output or use the variables as needed
#echo "Function Details:"
echo "$function_details"
#echo "Total Count:"
#echo "$total_count"
