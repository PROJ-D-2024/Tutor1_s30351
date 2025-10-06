#!/usr/bin/env python3
"""
Simple Text Processor Script
A basic script for processing text files with various operations.
"""

import argparse
import sys
import os

def count_words(filename):
    """Count words in a text file."""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()
            words = content.split()
            return len(words)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def count_lines(filename):
    """Count lines in a text file."""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            return len(lines)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def display_file_info(filename):
    """Display basic information about a text file."""
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' does not exist.")
        return
    
    print(f"File: {filename}")
    print(f"Size: {os.path.getsize(filename)} bytes")
    
    word_count = count_words(filename)
    line_count = count_lines(filename)
    
    if word_count is not None:
        print(f"Words: {word_count}")
    if line_count is not None:
        print(f"Lines: {line_count}")

def main():
    parser = argparse.ArgumentParser(
        description='A simple text processor for basic file operations.',
        prog='text_processor.py',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python text_processor.py --words example.txt    # Count words in file
  python text_processor.py --lines example.txt    # Count lines in file
  python text_processor.py --info example.txt     # Show file information
        '''
    )
    
    parser.add_argument('filename', nargs='?', help='Path to the text file to process')
    parser.add_argument('--words', action='store_true', help='Count words in the file')
    parser.add_argument('--lines', action='store_true', help='Count lines in the file')
    parser.add_argument('--info', action='store_true', help='Display file information')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    
    # Parse arguments
    args = parser.parse_args()
    
    # If no filename provided and no action specified, show help
    if not args.filename and not any([args.words, args.lines, args.info]):
        parser.print_help()
        return
    
    # If filename provided but no action, default to info
    if args.filename and not any([args.words, args.lines, args.info]):
        args.info = True
    
    # Perform requested operations
    if args.filename:
        if args.words:
            word_count = count_words(args.filename)
            if word_count is not None:
                print(f"Word count: {word_count}")
        
        elif args.lines:
            line_count = count_lines(args.filename)
            if line_count is not None:
                print(f"Line count: {line_count}")
        
        elif args.info:
            display_file_info(args.filename)
    else:
        print("Error: Please provide a filename.")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()

