# Tutor3: Data Standardization and Code Optimization

A comprehensive data cleaning and standardization pipeline with database integration for thesis research projects. This project implements professional-grade ETL (Extract, Transform, Load) operations with modular, reusable code and advanced data preprocessing techniques.


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

### Database Configuration
- `host`: PostgreSQL server hostname
- `port`: PostgreSQL server port (default: 5432)
- `database_name`: Target database name
- `username`: Database username
- `password`: Database password
- `schema`: Database schema (default: public)

### Cleaning Options
- `remove_duplicates`: Remove duplicate rows (default: true)
- `handle_missing_values`: Auto-handle missing values (default: true)
- `outlier_detection_method`: "IQR" or "zscore"
- `outlier_threshold`: Sensitivity for outlier detection (1.5 for IQR, 3 for Z-score)
- `standardize_categorical`: Normalize text data (default: true)

### Standardization Options
- `normalize_numerical`: Apply numerical scaling (default: true)
- `normalization_method`: "minmax", "zscore", or "robust"
- `encode_categorical`: Encode categorical variables (default: false)
- `encoding_method`: "label" or "onehot"
- `standardize_dates`: Convert dates to uniform format (default: true)
- `date_format`: "ISO", "US", or "EU"

## Data Standardization Methods

### Min-Max Scaling
Scales data to a fixed range (typically 0-1):
```
X_scaled = (X - X_min) / (X_max - X_min)
```
**Use case**: When you need bounded values in a specific range

### Z-Score Normalization
Standardizes data to have mean=0 and std=1:
```
X_normalized = (X - mean) / std
```
**Use case**: When your data follows normal distribution

### Robust Scaling
Uses median and IQR (resistant to outliers):
```
X_scaled = (X - median) / IQR
```
**Use case**: When your data contains outliers

## Module Documentation

### database_manager.py
Main class for all database operations with connection pooling.

**Key Methods:**
- `get_connection()`: Get connection from pool
- `create_table_from_dataframe()`: Create table from DataFrame schema
- `insert_dataframe()`: Batch insert DataFrame (1000 rows/batch)
- `update_dataframe()`: Update records from DataFrame
- `read_table()`: Read table into DataFrame
- `execute_query()`: Execute custom SQL query

### data_cleaning.py
Comprehensive data cleaning with multiple strategies.

**Key Methods:**
- `clean_dataframe()`: Apply all cleaning operations
- `remove_duplicates()`: Remove duplicate rows
- `handle_missing_values()`: Handle missing data (auto, drop, mean, median, mode)
- `detect_and_handle_outliers()`: Outlier detection and handling (IQR, Z-score)
- `correct_data_types()`: Auto-correct data types
- `standardize_categorical()`: Normalize text data

### data_standardization.py
Data standardization and normalization with multiple methods.

**Key Methods:**
- `standardize_dataframe()`: Apply all standardization
- `normalize_numerical_data()`: Normalize numerical columns
- `minmax_scaling()`: Min-Max scaling
- `zscore_normalization()`: Z-score normalization
- `robust_scaling()`: Robust scaling
- `standardize_dates()`: Uniform date formatting
- `encode_categorical_data()`: Encode categorical variables

## Security Best Practices

1. **Never commit credentials**: `config/database_config.json` is gitignored
2. **Use environment variables**: Consider using `.env` files for sensitive data
3. **Restrict database permissions**: Use appropriate user roles
4. **Validate input data**: All user input is validated
5. **Use connection pooling**: Prevents connection exhaustion attacks
6. **Parameterized queries**: Protection against SQL injection

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
