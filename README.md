# Tutor3: Data Standardization and Code Optimization

A comprehensive data cleaning and standardization pipeline with database integration for thesis research projects. This project implements professional-grade ETL (Extract, Transform, Load) operations with modular, reusable code.

## Project Overview

This repository implements **Task 2 (Tutor3)** focusing on:
- Data standardization (normalization, scaling, date formatting)
- Code optimization and modular architecture
- Database integration with PostgreSQL
- Automated data pipeline with error handling
- Comprehensive documentation and best practices

## Features

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

## Configuration Options

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

## Data Standardization Methods

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


## Testing

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
