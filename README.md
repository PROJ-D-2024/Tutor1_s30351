# Tutor2: Data Cleaning and Standardization for Thesis Project

A comprehensive data preprocessing pipeline for thesis projects using PostgreSQL database integration. This project provides tools for cleaning, standardizing, and preparing research data for analysis and modeling.

## Project Overview

Tutor2 is designed to help students prepare their thesis data by implementing industry-standard data cleaning and preprocessing techniques. The project uses PostgreSQL as the primary database and provides a complete pipeline for data ingestion, cleaning, and export.

## Features

### Data Cleaning Capabilities
- **Missing Data Handling**: Multiple strategies (drop, mean, median, mode imputation)
- **Duplicate Removal**: Automatic detection and removal of duplicate records
- **Outlier Detection**: IQR-based outlier detection and handling
- **Data Type Correction**: Automatic type inference and conversion
- **Categorical Standardization**: Text normalization and standardization

### Database Integration
- **PostgreSQL Support**: Full database integration with connection management
- **Data Pipeline**: Automated ETL (Extract, Transform, Load) operations
- **Version Control**: Git-based workflow with proper branching strategy

### Configuration Management
- **JSON Configuration**: Flexible configuration system for database and cleaning options
- **Environment Isolation**: Secure credential management with .gitignore protection

## Project Structure

```
Tutor2_s30351/
├── config/
│   ├── database_config.json          # Database connection settings
│   └── database_config.json.example  # Configuration template
├── scripts/
│   └── data_pipeline.py              # Main pipeline script
├── utils/
│   ├── database.py                   # Database connection utilities
│   └── data_cleaning.py              # Data cleaning functions
├── data/
│   ├── raw/                          # Raw data storage
│   ├── processed/                    # Processed data output
│   └── cleaned/                      # Final cleaned data
├── .gitignore                        # Git ignore rules
└── README.md                         # This file
```

## Prerequisites

- **Python 3.8+**
- **PostgreSQL 12+**
- **Required Python packages**:
  ```bash
  pip install psycopg2-binary pandas numpy scipy
  ```

## Setup Instructions

### 1. Database Setup

1. Install and configure PostgreSQL
2. Create a new database for your thesis project:
   ```sql
   CREATE DATABASE thesis_data;
   ```

### 2. Configuration Setup

1. Copy the configuration template:
   ```bash
   cp config/database_config.json.example config/database_config.json
   ```

2. Update `config/database_config.json` with your database credentials:
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

### 3. Repository Setup

1. Clone the repository and create a feature branch:
   ```bash
   git checkout -b feature/data-cleaning-setup
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

Run the pipeline with sample data:

### Basic Pipeline Operations

```bash
# Load raw data into database
python scripts/data_pipeline.py --load data/thesis_data.csv

# Process and clean data
python scripts/data_pipeline.py --process

# Export cleaned data
python scripts/data_pipeline.py --export data/cleaned_output.csv

# Run complete pipeline
python scripts/data_pipeline.py --full data/thesis_data.csv data/cleaned_output.csv
```

### Advanced Usage

```bash
# Load data with custom configuration
python scripts/data_pipeline.py --load data/thesis_data.xlsx

# Process with specific cleaning options (modify config first)
python scripts/data_pipeline.py --process

# Export to different formats
python scripts/data_pipeline.py --export data/cleaned_data.xlsx
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
- `remove_duplicates`: Enable duplicate removal (default: true)
- `handle_missing_values`: Enable missing value handling (default: true)
- `outlier_detection_method`: Method for outlier detection (IQR, Z-score)
- `outlier_threshold`: Threshold for outlier detection (default: 1.5)
- `standardize_categorical`: Enable categorical data standardization (default: true)
- `normalize_numerical`: Enable numerical data normalization (default: false)

## Data Cleaning Methods

### Missing Values Handling
- **Auto Strategy**: Automatically chooses best method based on data type and missing percentage
- **Drop**: Remove rows/columns with missing values
- **Mean/Median**: Impute numerical values with mean or median
- **Mode**: Impute categorical values with most frequent value

### Outlier Detection
- **IQR Method**: Uses Interquartile Range for outlier detection
- **Configurable Threshold**: Adjustable sensitivity for outlier detection

### Data Standardization
- **Categorical Data**: Text normalization, whitespace handling, common abbreviation replacement
- **Numerical Data**: Optional normalization and scaling

## Git Workflow

1. **Feature Development**: Create feature branches for new functionality
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Commits**: Use descriptive commit messages
   ```bash
   git add .
   git commit -m "Add data cleaning pipeline for thesis project"
   ```

3. **Pull Requests**: Create PRs for code review
   ```bash
   git push origin feature/your-feature-name
   # Create PR via GitHub interface
   ```

4. **Merge**: Merge approved PRs to main branch

## Example Thesis Workflow

1. **Data Collection**: Gather raw data for your thesis
2. **Initial Load**: Load raw data into PostgreSQL
3. **Data Cleaning**: Apply cleaning transformations
4. **Quality Check**: Verify data quality and completeness
5. **Export**: Export cleaned data for analysis
6. **Version Control**: Commit all changes with meaningful messages

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is for educational purposes as part of the thesis requirements.

## Support

For issues and questions:
- Review the configuration examples
- Check database connection settings
- Ensure all dependencies are installed
- Verify data file formats are supported
