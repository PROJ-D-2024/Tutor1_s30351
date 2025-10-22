# Tutor3: Data Standardization and Code Optimization

A comprehensive data cleaning and standardization pipeline with database integration for thesis research projects. This project implements professional-grade ETL (Extract, Transform, Load) operations with modular, reusable code.

## 📋 Project Overview

This repository implements **Task 2 (Tutor3)** focusing on:
- Data standardization (normalization, scaling, date formatting)
- Code optimization and modular architecture
- Database integration with PostgreSQL
- Automated data pipeline with error handling
- Comprehensive documentation and best practices

## 🚀 Features

### Data Cleaning Capabilities
- **Missing Data Handling**: Multiple strategies (drop, mean, median, mode imputation)
- **Duplicate Removal**: Automatic detection and removal of duplicate records
- **Outlier Detection**: IQR and Z-score based outlier detection and handling
- **Data Type Correction**: Automatic type inference and conversion
- **Categorical Standardization**: Text normalization and standardization

### Data Standardization Features
- **Normalization Methods**:
  - Min-Max Scaling (0-1 range)
  - Z-Score Normalization (standard normal distribution)
  - Robust Scaling (using median and IQR, resistant to outliers)
- **Date/Time Formatting**: ISO 8601, US, and European date formats
- **Categorical Encoding**: Label encoding and one-hot encoding
- **Reversible Transformations**: Save and load fitted scalers

### Database Integration
- **PostgreSQL Support**: Full database integration with connection pooling
- **Efficient Queries**: Optimized INSERT, UPDATE, and SELECT operations
- **Batch Operations**: Efficient batch inserts for large datasets
- **Transaction Management**: Proper error handling and rollback support
- **Connection Pooling**: Resource-efficient connection management

### Pipeline Automation
- **ETL Pipeline**: Automated Extract, Transform, Load operations
- **Flexible I/O**: Support for CSV and database sources/destinations
- **Command-Line Interface**: Easy-to-use CLI for common operations
- **Logging**: Comprehensive logging to file and console
- **Error Handling**: Robust error handling throughout the pipeline

## 📁 Project Structure

```
Tutor1_s30351/
├── config/
│   └── database_config.json      # Database and pipeline configuration
├── data/
│   ├── raw/                      # Raw input data
│   │   └── sample_thesis_data.csv
│   ├── processed/                # Intermediate processed data
│   └── cleaned/                  # Final cleaned data
├── scripts/
│   └── data_pipeline.py          # Main pipeline script
├── database_manager.py           # Database operations module
├── data_cleaning.py              # Data cleaning functions
├── data_standardization.py       # Normalization and standardization
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── text_processor.py             # Legacy text processing utility
```

## 🛠️ Installation

### Prerequisites
- Python 3.8 or higher
- PostgreSQL 12 or higher
- pip package manager

### Setup Instructions

1. **Clone the repository**:
```bash
git clone <repository-url>
cd Tutor1_s30351
```

2. **Create a virtual environment** (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure database**:
Edit `config/database_config.json` with your PostgreSQL credentials:
```json
{
  "database": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database_name": "thesis_data",
    "username": "your_username",
    "password": "your_password",
    "schema": "public"
  }
}
```

⚠️ **Important**: The `database_config.json` file should be added to `.gitignore` to protect credentials.

## 📖 Usage

### Command-Line Interface

The pipeline script provides several commands for different operations:

#### 1. Load CSV to Database
```bash
python scripts/data_pipeline.py --load data/raw/sample_thesis_data.csv --table raw_data
```

#### 2. Process and Clean Data
```bash
python scripts/data_pipeline.py --process raw_data --table cleaned_data
```

#### 3. Export Database Table to CSV
```bash
python scripts/data_pipeline.py --export cleaned_data --csv data/cleaned/output.csv
```

#### 4. Run Full Pipeline
```bash
python scripts/data_pipeline.py --full data/raw/sample_thesis_data.csv \
  --csv data/cleaned/output.csv \
  --table cleaned_data
```

### Python API

You can also use the modules directly in your Python code:

```python
from database_manager import DatabaseManager
from data_cleaning import DataCleaner
from data_standardization import DataStandardizer
import pandas as pd

# Load configuration
db_manager = DatabaseManager("config/database_config.json")

# Read data
df = pd.read_csv("data/raw/sample_thesis_data.csv")

# Clean data
cleaner = DataCleaner({
    'remove_duplicates': True,
    'handle_missing_values': True,
    'outlier_detection_method': 'IQR'
})
df_cleaned = cleaner.clean_dataframe(df)

# Standardize data
standardizer = DataStandardizer({
    'normalize_numerical': True,
    'normalization_method': 'minmax'
})
df_standardized = standardizer.standardize_dataframe(df_cleaned)

# Load to database
db_manager.create_table_from_dataframe(df_standardized, 'thesis_data', drop_if_exists=True)
db_manager.insert_dataframe(df_standardized, 'thesis_data')

# Cleanup
db_manager.close_all_connections()
```

## ⚙️ Configuration Options

### Cleaning Options
```json
{
  "cleaning_options": {
    "remove_duplicates": true,
    "handle_missing_values": true,
    "outlier_detection_method": "IQR",
    "outlier_threshold": 1.5,
    "standardize_categorical": true,
    "normalize_numerical": false
  }
}
```

- `remove_duplicates`: Remove duplicate rows
- `handle_missing_values`: Auto-handle missing values
- `outlier_detection_method`: "IQR" or "zscore"
- `outlier_threshold`: Sensitivity for outlier detection (1.5 for IQR, 3 for Z-score)
- `standardize_categorical`: Normalize text data
- `normalize_numerical`: Apply numerical normalization

### Standardization Options
```json
{
  "standardization_options": {
    "normalize_numerical": true,
    "normalization_method": "minmax",
    "encode_categorical": false,
    "encoding_method": "label",
    "standardize_dates": true,
    "date_format": "ISO"
  }
}
```

- `normalize_numerical`: Apply numerical scaling
- `normalization_method`: "minmax", "zscore", or "robust"
- `encode_categorical`: Encode categorical variables
- `encoding_method`: "label" or "onehot"
- `standardize_dates`: Convert dates to uniform format
- `date_format`: "ISO", "US", or "EU"

## 📊 Data Standardization Methods

### Min-Max Scaling
Scales data to a fixed range (typically 0-1):
```
X_scaled = (X - X_min) / (X_max - X_min)
```

### Z-Score Normalization
Standardizes data to have mean=0 and std=1:
```
X_normalized = (X - mean) / std
```

### Robust Scaling
Uses median and IQR (resistant to outliers):
```
X_scaled = (X - median) / IQR
```

## 🔒 Security Best Practices

1. **Never commit credentials**: Add `config/database_config.json` to `.gitignore`
2. **Use environment variables**: Consider using `.env` files for sensitive data
3. **Restrict database permissions**: Use read-only accounts where possible
4. **Validate input data**: Always validate and sanitize user input
5. **Use connection pooling**: Prevents connection exhaustion attacks

## 🧪 Testing

Run the pipeline with sample data:

```bash
# Test with provided sample data
python scripts/data_pipeline.py --full data/raw/sample_thesis_data.csv \
  --csv data/cleaned/test_output.csv \
  --table test_cleaned_data
```

The sample data includes:
- 35 student records
- Missing values (for testing imputation)
- Duplicates (for testing deduplication)
- Outliers (for testing outlier detection)
- Inconsistent date formats (for testing standardization)

## 📝 Logging

The pipeline creates detailed logs in `data_pipeline.log`:

```bash
# View real-time logs
tail -f data_pipeline.log
```

Log levels:
- **INFO**: Normal operations and progress
- **WARNING**: Non-critical issues
- **ERROR**: Critical errors requiring attention
- **DEBUG**: Detailed diagnostic information

## 🤝 Development Workflow

### Branch Strategy

This project uses feature branches for development:

```bash
# Create feature branch
git checkout -b data-standardization

# Make changes and commit
git add .
git commit -m "feat: implement Min-Max scaling"

# Push to remote
git push origin data-standardization

# Create pull request for review
```

### Commit Message Convention

Follow conventional commits format:

- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `refactor:` Code refactoring
- `test:` Adding tests
- `chore:` Maintenance tasks

Examples:
```bash
git commit -m "feat: add Z-score normalization"
git commit -m "fix: handle missing values in date columns"
git commit -m "docs: update README with usage examples"
```

## 🐛 Troubleshooting

### Database Connection Issues

**Error**: `Connection refused`
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql  # Linux
brew services list  # macOS
```

**Error**: `Authentication failed`
- Verify credentials in `config/database_config.json`
- Check PostgreSQL user permissions

### Module Import Errors

**Error**: `ModuleNotFoundError`
```bash
# Ensure you're in the correct directory
cd /path/to/Tutor1_s30351

# Install dependencies
pip install -r requirements.txt
```

### Data Issues

**Error**: `File not found`
- Verify file path is correct
- Check that data directories exist

**Error**: `Parsing error`
- Verify CSV format and encoding
- Check for special characters

## 📚 Module Documentation

### database_manager.py
- `DatabaseManager`: Main class for database operations
  - `get_connection()`: Get connection from pool
  - `create_table_from_dataframe()`: Create table from DataFrame schema
  - `insert_dataframe()`: Batch insert DataFrame
  - `update_dataframe()`: Update records from DataFrame
  - `read_table()`: Read table into DataFrame
  - `execute_query()`: Execute custom SQL query

### data_cleaning.py
- `DataCleaner`: Comprehensive data cleaning
  - `clean_dataframe()`: Apply all cleaning operations
  - `remove_duplicates()`: Remove duplicate rows
  - `handle_missing_values()`: Handle missing data
  - `detect_and_handle_outliers()`: Outlier detection and handling
  - `correct_data_types()`: Auto-correct data types
  - `standardize_categorical()`: Normalize text data

### data_standardization.py
- `DataStandardizer`: Data standardization and normalization
  - `standardize_dataframe()`: Apply all standardization
  - `normalize_numerical_data()`: Normalize numerical columns
  - `minmax_scaling()`: Min-Max scaling
  - `zscore_normalization()`: Z-score normalization
  - `robust_scaling()`: Robust scaling
  - `standardize_dates()`: Uniform date formatting
  - `encode_categorical_data()`: Encode categorical variables

## 🎯 Task 2 Requirements Checklist

### Git Repository Setup (5 Points) ✅
- [x] Separate branch (`data-standardization`)
- [x] Consistent commit message format
- [x] Organized repository structure with modules
- [x] Pull request workflow ready

### Database Interaction (5 Points) ✅
- [x] Update data in database (INSERT, UPDATE, DELETE)
- [x] Automated data loading scripts
- [x] Efficient SQL queries with connection pooling
- [x] Credentials in excluded configuration file

### Data Standardization and Code Quality (10 Points) ✅
- [x] Normalization methods (Min-Max, Z-score, Robust)
- [x] Categorical data standardization
- [x] Date/time formatting (ISO format)
- [x] Reusable, modular code structure
- [x] Comprehensive error handling
- [x] Code comments and documentation
- [x] Updated README.md with instructions

## 📄 License

This project is part of academic coursework at PJATK (Polish-Japanese Academy of Information Technology).

## 👥 Contributors

- Student ID: s30351
- Project: PRO - Tutor3
- Course: WIS-05-2025

## 📞 Support

For issues or questions:
1. Check the troubleshooting section
2. Review the logs in `data_pipeline.log`
3. Consult the module documentation
4. Contact the course instructor

---

**Last Updated**: October 2025  
**Version**: 3.0 (Tutor3 - Data Standardization)
