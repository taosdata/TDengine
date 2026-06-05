#!/bin/bash

# Pass version, ip, and file paths as arguments
version="$1"
ip="$2"
shift 2  # Shift the first two arguments to get file paths
file_paths="$@"

# Execute awk and capture the output
readarray -t output < <(awk -v version="$version" -v ip="$ip" '
BEGIN {
    RS = "\\n";  # Set the record separator to newline
    total = 0;   # Initialize total count
    version_regex = "\"version\":\"" version;  # Construct the regex for version
    ip_regex = "\"ip\":\"" ip "\"";  # Construct the regex for IP
}
{
    found = 0;  # Initialize the found flag to false
    start_collecting = 1;  # Start collecting by default, unless taosAssertDebug is encountered
    split($0, parts, "\\n");  # Split each record by newline

    # Check for version and IP in each part
    version_matched = 0;
    ip_excluded = 0;
    for (i in parts) {
        if (parts[i] ~ version_regex) {
            version_matched = 1;  # Set flag if version is matched
        }
        if (parts[i] ~ ip_regex) {
            ip_excluded = 1;  # Set flag if IP is matched
            break;  # No need to continue if IP is excluded
        }
    }

    # Process only if version is matched and IP is not excluded
    if (version_matched && !ip_excluded) {
        for (i in parts) {
            if (parts[i] ~ /taosAssertDebug/) {
                start_collecting = 0;  # Skip this record if taosAssertDebug is encountered
                break;  # Exit the loop
            }
        }
        if (start_collecting == 1) {  # Continue processing if taosAssertDebug is not found
            for (i in parts) {
                if (found == 0 && parts[i] ~ /frame:.*taosd\([^)]+\)/) {
                    # Match the first frame that meets the condition
                    match(parts[i], /taosd\(([^)]+)\)/, a);  # Extract the function name
                    if (a[1] != "") {
                        count[a[1]]++;  # Increment the count for this function name
                        total++;  # Increment the total count
                        found = 1;  # Set found flag to true
                        break;  # Exit the loop once the function is found
                    }
                }
            }
        }
    }
}
END {
    for (c in count) {
        printf "%d %s\n", count[c], c;  # Print the count and function name formatted
    }
    print total;  # Print the total count alone
}' $file_paths)  # Note the removal of quotes around "$file_paths" to handle multiple paths

# Capture the function details and total count into separate variables
function_details=$(printf "%s\n" "${output[@]::${#output[@]}-1}")  # Join array elements with newlines
total_count="${output[-1]}"  # The last element

# Output or use the variables as needed
#echo "Function Details:"
echo "$function_details"
#echo "Total Count:"
#echo "$total_count"
