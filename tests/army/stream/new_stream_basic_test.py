#!/usr/bin/env python3

import os
import sys
import subprocess

def main():
    valid_option_val = ["interval", "state", "session", "count", "event"]

    if len(sys.argv) != 2:
        print("Usage: ./new_stream_basic_test.py <option>")
        sys.exit(1)

    opt = sys.argv[1]

    if opt not in valid_option_val:
        print(f"Invalid option: {opt}. Valid options are: {', '.join(valid_option_val)}")
        sys.exit(1)

    try:
        with open("basic_test.template", "r") as f:
            template = f.read()
    except FileNotFoundError:
        print("Template file not found.")
        sys.exit(1)
    except Exception as e:
        print("Error reading template file: {e}")
        sys.exit(1)

    placeholder = "%WINDOW%"
    if opt == "interval":
        template = template.replace(placeholder, "interval (1s) sliding (1s)")
    elif opt == "state":
        template = template.replace(placeholder, "state_window (id)")
    elif opt == "session":
        template = template.replace(placeholder, "session (id, 1s)")
    elif opt == "count":
        template = template.replace(placeholder, "count_window (1)")
    elif opt == "event":
        template = template.replace(placeholder, "event_window (start with id > 0 end with id > 0)")

    try:
        with open("basic_test.sql", "w") as f:
            f.write(template)
        command = ["taos", "-f", "basic_test.sql"]
        result = subprocess.run(command, capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
    except Exception as e:
        print(f"Error executing command: {e}")
        sys.exit(1)
    finally:
        os.remove("basic_test.sql")

if __name__ == "__main__":
    main()
