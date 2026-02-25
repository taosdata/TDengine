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

def reorder_csv_by_column(input_file, output_file, sort_columns):
    """
    Read CSV file and reorder rows by specified columns, then write to new file
    
    Args:
        input_file: Input CSV file path
        output_file: Output CSV file path
        sort_columns: List of column names to sort by
    """
    try:
        # Read CSV file
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            # Check if all sort columns exist
            for col in sort_columns:
                if col not in headers:
                    print(f"Error: Column '{col}' not found in CSV headers")
                    print(f"Available columns: {', '.join(headers)}")
                    return False
            
            # Read all rows
            rows = list(reader)
            total_rows = len(rows)
            print(f"Read {total_rows} rows from {input_file}")
        
        # Sort rows by specified columns (handle None/empty values)
        print(f"Sorting by columns: {', '.join(sort_columns)}...")
        rows.sort(key=lambda row: tuple(row[col] or '' for col in sort_columns))
        
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
        description='Reorder CSV file rows by specified columns',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sort by single column 'name'
  python pipline_csv.py -i input.csv -o output.csv -c name
  
  # Sort by two columns 'name' and 'ts'
  python pipline_csv.py -i input.csv -o output.csv -c name ts
  
  # Sort by 'fleet' then 'name'
  python pipline_csv.py -i readings.csv -o sorted.csv -c fleet name
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
        '-c', '--columns',
        required=True,
        nargs='+',
        help='Column name(s) to sort by (space-separated for multiple columns)'
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
    print(f"Input file:    {args.input}")
    print(f"Output file:   {args.output}")
    print(f"Sort columns:  {', '.join(args.columns)}")
    print(f"{'='*60}\n")
    
    success = reorder_csv_by_column(args.input, args.output, args.columns)
    
    if success:
        print("\n✓ CSV reordering completed successfully")
        sys.exit(0)
    else:
        print("\n✗ CSV reordering failed")
        sys.exit(1)


'''
# Sort by single column
# python sort_csv.py -i head.csv -o output.csv -c name

# Sort by two columns (name first, then ts)
# python sort_csv.py -i head.csv -o output.csv -c name ts

# Sort by three columns (fleet, name, ts)
# python sort_csv.py -i head.csv -o output.csv -c fleet name ts
'''
if __name__ == "__main__":
    main()