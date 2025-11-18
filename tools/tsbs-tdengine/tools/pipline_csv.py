#
# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import csv
import argparse
import sys
import os

def reorder_csv_by_column(input_file, output_file, sort_column):
    """
    Read CSV file and reorder rows by specified column, then write to new file
    
    Args:
        input_file: Input CSV file path
        output_file: Output CSV file path
        sort_column: Column name to sort by
    """
    try:
        # Read CSV file
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            # Check if sort column exists
            if sort_column not in headers:
                print(f"Error: Column '{sort_column}' not found in CSV headers")
                print(f"Available columns: {', '.join(headers)}")
                return False
            
            # Read all rows
            rows = list(reader)
            total_rows = len(rows)
            print(f"Read {total_rows} rows from {input_file}")
        
        # Sort rows by specified column (handle None/empty values)
        print(f"Sorting by column '{sort_column}'...")
        rows.sort(key=lambda row: (row[sort_column] or ''))  # Treat None/empty as empty string
        
        # Write to output file
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"Successfully wrote {total_rows} rows to {output_file}")
        return True
        
    except FileNotFoundError:
        print(f"Error: File not found: {input_file}")
        return False
    except Exception as e:
        print(f"Error processing CSV: {e}")
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Reorder CSV file rows by specified column',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sort by 'name' column
  python pipline_csv.py -i input.csv -o output.csv -c name
  
  # Sort by 'ts' column (timestamp)
  python pipline_csv.py -i readings.csv -o sorted_readings.csv -c ts
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Input CSV file path'
    )
    
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Output CSV file path'
    )
    
    parser.add_argument(
        '-c', '--column',
        required=True,
        help='Column name to sort by'
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input):
        print(f"Error: Input file does not exist: {args.input}")
        sys.exit(1)
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Process CSV
    print(f"\n{'='*60}")
    print("CSV Reorder Tool")
    print(f"{'='*60}")
    print(f"Input file:  {args.input}")
    print(f"Output file: {args.output}")
    print(f"Sort column: {args.column}")
    print(f"{'='*60}\n")
    
    success = reorder_csv_by_column(args.input, args.output, args.column)
    
    if success:
        print("\n✓ CSV reordering completed successfully")
        sys.exit(0)
    else:
        print("\n✗ CSV reordering failed")
        sys.exit(1)

if __name__ == "__main__":
    main()