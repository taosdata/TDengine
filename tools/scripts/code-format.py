#!/usr/bin/env python3
"""
Code formatting script using clang-format to check and format C/C++ files recursively.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

def find_clang_format():
    """Find available clang-format binary."""
    # Try common clang-format versions
    versions = ['clang-format-14', 'clang-format-13', 'clang-format-12', 'clang-format-11', 'clang-format']
    
    for version in versions:
        try:
            result = subprocess.run([version, '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                return version
        except (subprocess.SubprocessError, FileNotFoundError):
            continue
    
    return None

def get_supported_extensions():
    """Get the file extensions supported by clang-format."""
    return {'.c', '.cpp', '.cc', '.cxx', '.h', '.hpp', '.hh', '.hxx'}

def should_exclude_file(file_path, exclude_patterns):
    """Check if a file should be excluded based on patterns."""
    if not exclude_patterns:
        return False
    
    file_path_str = str(file_path)
    for pattern in exclude_patterns:
        if pattern in file_path_str:
            return True
    return False

def find_source_files(directory, exclude_patterns=None):
    """Recursively find C/C++ source files in directory."""
    if exclude_patterns is None:
        exclude_patterns = []
    
    source_files = []
    extensions = get_supported_extensions()
    
    for root, dirs, files in os.walk(directory):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            file_path = Path(root) / file
            if file_path.suffix.lower() in extensions:
                if not should_exclude_file(file_path, exclude_patterns):
                    source_files.append(file_path)
    
    return source_files

def check_file_formatting(clang_format, file_path, style_file=None):
    """Check if a file is properly formatted."""
    cmd = [clang_format, '--dry-run', '--Werror']
    if style_file and os.path.exists(style_file):
        cmd.extend(['--style=file', f'--assume-filename={style_file}'])
    
    cmd.append(str(file_path))
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0, result.stderr
    except subprocess.SubprocessError as e:
        return False, str(e)

def format_file(clang_format, file_path, style_file=None):
    """Format a file in-place."""
    cmd = [clang_format, '-i']
    if style_file and os.path.exists(style_file):
        cmd.extend(['--style=file', f'--assume-filename={style_file}'])
    
    cmd.append(str(file_path))
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0, result.stderr
    except subprocess.SubprocessError as e:
        return False, str(e)

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Code formatting script using clang-format to check and format C/C++ files recursively.'
    )
    
    parser.add_argument(
        'directory',
        type=str,
        help='Directory to process recursively'
    )
    
    parser.add_argument(
        '--exclude',
        type=str,
        nargs='+',
        default=[],
        help='Files or paths to exclude (substring matching)'
    )
    
    parser.add_argument(
        '--format',
        action='store_true',
        help='Format files in-place (default: check only)'
    )
    
    parser.add_argument(
        '--style-file',
        type=str,
        default='.clang-format',
        help='Path to the code format configuration file'
    )
    
    parser.add_argument(
        '--clang-format',
        type=str,
        help='Path to clang-format binary (auto-detected if not specified)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print verbose output'
    )
    
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()
    
    # Find clang-format binary
    if args.clang_format:
        clang_format = args.clang_format
    else:
        clang_format = find_clang_format()
    
    if not clang_format:
        print("Error: clang-format not found. Please install clang-format or specify path with --clang-format")
        sys.exit(1)
    
    if args.verbose:
        print(f"Using clang-format: {clang_format}")
    
    # Check if directory exists
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        sys.exit(1)
    
    # Find source files
    if args.verbose:
        print(f"Scanning directory: {args.directory}")
        if args.exclude:
            print(f"Excluding patterns: {args.exclude}")
    
    source_files = find_source_files(args.directory, args.exclude)
    
    if not source_files:
        print("No C/C++ source files found to process")
        return
    
    if args.verbose:
        print(f"Found {len(source_files)} files to process")
    
    # Process files
    failed_files = []
    processed_count = 0
    
    for file_path in source_files:
        if args.verbose:
            print(f"Processing: {file_path}")
        
        if args.format:
            # Format file
            success, error = format_file(clang_format, file_path, args.style_file)
            if success:
                processed_count += 1
                if args.verbose:
                    print(f"  ✓ Formatted: {file_path}")
            else:
                failed_files.append((file_path, error))
                if args.verbose:
                    print(f"  ✗ Failed to format: {file_path}")
                    if error:
                        print(f"    Error: {error}")
        else:
            # Check formatting
            success, error = check_file_formatting(clang_format, file_path, args.style_file)
            if success:
                processed_count += 1
                if args.verbose:
                    print(f"  ✓ Properly formatted: {file_path}")
            else:
                failed_files.append((file_path, error))
                if args.verbose:
                    print(f"  ✗ Not properly formatted: {file_path}")
                    if error:
                        print(f"    Error: {error}")
    
    # Print summary
    print(f"\nSummary:")
    print(f"  Total files processed: {len(source_files)}")
    print(f"  Successfully processed: {processed_count}")
    print(f"  Failed: {len(failed_files)}")
    
    if failed_files:
        print(f"\nFailed files:")
        for file_path, error in failed_files:
            print(f"  {file_path}")
            if error and args.verbose:
                print(f"    Error: {error}")
        
        if not args.format:
            print(f"\nTo fix formatting issues, run with --format flag")
        
        sys.exit(1)
    else:
        if args.format:
            print(f"✓ All files successfully formatted")
        else:
            print(f"✓ All files are properly formatted")

if __name__ == '__main__':
    main()
