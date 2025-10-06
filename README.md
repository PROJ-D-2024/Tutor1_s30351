# Text Processor Script

A simple Python script for processing text files with basic operations like word counting, line counting, and file information display.

## Features

- Count words in text files
- Count lines in text files
- Display basic file information (size, word count, line count)
- Command-line interface with help functionality
- Error handling for missing files and read errors

## Usage

```bash
# Show help message
python text_processor.py --help

# Display file information (default behavior)
python text_processor.py example.txt

# Count words in a file
python text_processor.py --words example.txt

# Count lines in a file
python text_processor.py --lines example.txt

# Show detailed file information
python text_processor.py --info example.txt
```