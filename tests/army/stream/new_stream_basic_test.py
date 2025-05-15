#!/usr/bin/env python3

import argparse
import os
import sys
import subprocess

def main():
    valid_window_type = ["interval", "state", "session", "count", "event"]

    parser = argparse.ArgumentParser(description="Generate and execute a SQL test script.")
    parser.add_argument("window_type", choices=valid_window_type, help="Type of window to use in the SQL script.")
    parser.add_argument("--cache", action="store_true", help="Enable cache for the SQL script.")
    parser.add_argument("--history", action="store_true", help="Enable history for the SQL script.")
    parser.add_argument("--part", action="store_true", help="Enable partition for the SQL script.")
    parser.add_argument("--gen", action="store_true", help="Generate the SQL script without executing it.")

    args = parser.parse_args()
    window_type = args.window_type
    cache = args.cache
    history = args.history
    part = args.part
    gen = args.gen

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
    if window_type == "interval":
        template = template.replace(placeholder, "interval (1s) sliding (1s)")
    elif window_type == "state":
        template = template.replace(placeholder, "state_window (id)")
    elif window_type == "session":
        template = template.replace(placeholder, "session (ts, 1s)")
    elif window_type == "count":
        template = template.replace(placeholder, "count_window (1)")
    elif window_type == "event":
        template = template.replace(placeholder, "event_window (start with id > 0 end with id > 0)")

    placeholder = "%CALC_SOURCE%"
    if cache:
        template = template.replace(placeholder, "%%trows")
    else:
        template = template.replace(placeholder, "stream_query")

    placeholder = "%OPTIONS%"
    if history:
        template = template.replace(placeholder, "options(fill_history_first 1)")
    else:
        template = template.replace(placeholder, "")

    placeholder = "%TRIGGER_SOURCE%"
    if part:
        template = template.replace(placeholder, "stream_trigger_st partition by tbname")
    else:
        template = template.replace(placeholder, "stream_trigger")

    try:
        with open("basic_test.sql", "w") as f:
            f.write(template)
        if gen:
            print("SQL script generated successfully. Exiting without execution.")
            return
        command = ["taos", "-f", "basic_test.sql"]
        result = subprocess.run(command, capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
    except Exception as e:
        print(f"Error executing command: {e}")
        sys.exit(1)
    finally:
        if not gen:
            os.remove("basic_test.sql")

if __name__ == "__main__":
    main()
