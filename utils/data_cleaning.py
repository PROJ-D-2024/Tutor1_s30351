"""
Data cleaning utilities for thesis project
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
from scipy import stats


class DataCleaner:
    """Data cleaning utilities for thesis project"""

    def __init__(self, config: Dict[str, Any]):
        """Initialize data cleaner with configuration"""
        self.config = config
        self.cleaning_options = config.get('cleaning_options', {})

    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load data from various file formats"""
        file_extension = file_path.split('.')[-1].lower()

        try:
            if file_extension == 'csv':
                return pd.read_csv(file_path)
            elif file_extension in ['xlsx', 'xls']:
                return pd.read_excel(file_path)
            elif file_extension == 'json':
                return pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        except Exception as e:
            print(f"Error loading data from {file_path}: {e}")
            raise

    def save_data(self, df: pd.DataFrame, file_path: str) -> bool:
        """Save cleaned data to file"""
        try:
            file_extension = file_path.split('.')[-1].lower()

            if file_extension == 'csv':
                df.to_csv(file_path, index=False)
            elif file_extension in ['xlsx', 'xls']:
                df.to_excel(file_path, index=False)
            elif file_extension == 'json':
                df.to_json(file_path, orient='records')
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")

            print(f"Data saved successfully to {file_path}")
            return True
        except Exception as e:
            print(f"Error saving data to {file_path}: {e}")
            return False

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values based on configuration"""
        if not self.cleaning_options.get('handle_missing_values', False):
            return df

        df_cleaned = df.copy()
        missing_strategy = self.cleaning_options.get('missing_strategy', 'auto')

        for column in df.columns:
            missing_count = df[column].isnull().sum()

            if missing_count > 0:
                print(f"Column '{column}' has {missing_count} missing values")

                if missing_strategy == 'drop':
                    df_cleaned = df_cleaned.dropna(subset=[column])
                    print(f"Dropped rows with missing values in '{column}'")
                elif missing_strategy == 'mean' and df[column].dtype in ['int64', 'float64']:
                    mean_value = df[column].mean()
                    df_cleaned[column] = df_cleaned[column].fillna(mean_value)
                    print(f"Filled missing values in '{column}' with mean: {mean_value}")
                elif missing_strategy == 'median' and df[column].dtype in ['int64', 'float64']:
                    median_value = df[column].median()
                    df_cleaned[column] = df_cleaned[column].fillna(median_value)
                    print(f"Filled missing values in '{column}' with median: {median_value}")
                elif missing_strategy == 'mode':
                    mode_value = df[column].mode().iloc[0] if not df[column].mode().empty else df[column].iloc[0]
                    df_cleaned[column] = df_cleaned[column].fillna(mode_value)
                    print(f"Filled missing values in '{column}' with mode: {mode_value}")
                elif missing_strategy == 'auto':
                    # Auto strategy based on column type and missing percentage
                    missing_percentage = missing_count / len(df) * 100

                    if missing_percentage > 50:
                        # Drop column if more than 50% missing
                        df_cleaned = df_cleaned.drop(columns=[column])
                        print(f"Dropped column '{column}' due to high missing percentage ({missing_percentage".1f"}%)")
                    elif df[column].dtype in ['int64', 'float64']:
                        # Fill numerical columns with median
                        median_value = df[column].median()
                        df_cleaned[column] = df_cleaned[column].fillna(median_value)
                        print(f"Filled missing values in '{column}' with median: {median_value}")
                    else:
                        # Fill categorical columns with mode
                        mode_value = df[column].mode().iloc[0] if not df[column].mode().empty else "Unknown"
                        df_cleaned[column] = df_cleaned[column].fillna(mode_value)
                        print(f"Filled missing values in '{column}' with mode: {mode_value}")

        return df_cleaned

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows"""
        if not self.cleaning_options.get('remove_duplicates', False):
            return df

        initial_shape = df.shape
        df_cleaned = df.drop_duplicates()
        final_shape = df_cleaned.shape

        duplicates_removed = initial_shape[0] - final_shape[0]
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate rows")

        return df_cleaned

    def detect_and_handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and handle outliers using IQR method"""
        if not self.cleaning_options.get('detect_outliers', False):
            return df

        df_cleaned = df.copy()
        method = self.cleaning_options.get('outlier_detection_method', 'IQR')
        threshold = self.cleaning_options.get('outlier_threshold', 1.5)

        numerical_columns = df.select_dtypes(include=[np.number]).columns

        for column in numerical_columns:
            if method == 'IQR':
                outliers_handled = self._handle_outliers_iqr(df_cleaned, column, threshold)
                if outliers_handled > 0:
                    print(f"Handled {outliers_handled} outliers in column '{column}' using IQR method")

        return df_cleaned

    def _handle_outliers_iqr(self, df: pd.DataFrame, column: str, threshold: float) -> int:
        """Handle outliers using IQR method"""
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR

        outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
        outliers_count = len(outliers)

        if outliers_count > 0:
            # Replace outliers with bounds
            df[column] = np.where(df[column] < lower_bound, lower_bound, df[column])
            df[column] = np.where(df[column] > upper_bound, upper_bound, df[column])

        return outliers_count

    def correct_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Correct and optimize data types"""
        df_corrected = df.copy()

        for column in df.columns:
            current_dtype = df[column].dtype

            # Convert string dates to datetime
            if df[column].dtype == 'object':
                # Try to convert to datetime
                try:
                    # Check if column looks like dates
                    sample_values = df[column].dropna().head(10)
                    if len(sample_values) > 0:
                        # Try common date formats
                        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                            try:
                                pd.to_datetime(sample_values.iloc[0], format=fmt)
                                df_corrected[column] = pd.to_datetime(df[column], format=fmt, errors='coerce')
                                print(f"Converted '{column}' to datetime")
                                break
                            except (ValueError, TypeError):
                                continue
                except Exception:
                    pass

            # Convert strings that are actually numbers
            if df[column].dtype == 'object':
                try:
                    # Check if all non-null values are numeric
                    non_null_values = df[column].dropna()
                    if len(non_null_values) > 0:
                        pd.to_numeric(non_null_values.iloc[0])
                        all_numeric = pd.to_numeric(non_null_values, errors='coerce').notna().all()
                        if all_numeric:
                            df_corrected[column] = pd.to_numeric(df[column], errors='coerce')
                            print(f"Converted '{column}' to numeric")
                except Exception:
                    pass

        return df_corrected

    def standardize_categorical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize categorical data"""
        if not self.cleaning_options.get('standardize_categorical', False):
            return df

        df_standardized = df.copy()

        for column in df.columns:
            if df[column].dtype == 'object':
                # Convert to lowercase and strip whitespace
                df_standardized[column] = df[column].astype(str).str.lower().str.strip()

                # Replace common variations
                replacements = {
                    'yes': 'y', 'no': 'n',
                    'true': 'y', 'false': 'n',
                    '1': 'y', '0': 'n',
                    'male': 'm', 'female': 'f',
                    'm': 'm', 'f': 'f'
                }

                for old_val, new_val in replacements.items():
                    df_standardized[column] = df_standardized[column].replace(old_val, new_val)

                print(f"Standardized categorical data in '{column}'")

        return df_standardized

    def generate_summary_report(self, df_original: pd.DataFrame, df_cleaned: pd.DataFrame) -> Dict[str, Any]:
        """Generate a summary report of cleaning operations"""
        report = {
            'original_shape': df_original.shape,
            'cleaned_shape': df_cleaned.shape,
            'rows_removed': df_original.shape[0] - df_cleaned.shape[0],
            'columns_removed': df_original.shape[1] - df_cleaned.shape[1],
            'missing_values_before': df_original.isnull().sum().sum(),
            'missing_values_after': df_cleaned.isnull().sum().sum(),
            'duplicate_rows_removed': df_original.shape[0] - df_original.drop_duplicates().shape[0] - (df_cleaned.shape[0] - df_cleaned.drop_duplicates().shape[0])
        }

        return report


def clean_thesis_data(file_path: str, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Main function to clean thesis data"""
    cleaner = DataCleaner(config)

    print("Loading data...")
    df = cleaner.load_data(file_path)

    print("Original data shape:", df.shape)
    print("Original data info:")
    print(df.info())

    # Store original for comparison
    df_original = df.copy()

    # Apply cleaning steps
    print("\n=== Starting Data Cleaning ===")

    print("1. Correcting data types...")
    df = cleaner.correct_data_types(df)

    print("2. Handling missing values...")
    df = cleaner.handle_missing_values(df)

    print("3. Removing duplicates...")
    df = cleaner.remove_duplicates(df)

    print("4. Detecting and handling outliers...")
    df = cleaner.detect_and_handle_outliers(df)

    print("5. Standardizing categorical data...")
    df = cleaner.standardize_categorical_data(df)

    # Generate summary report
    report = cleaner.generate_summary_report(df_original, df)

    print("\n=== Cleaning Summary ===")
    print(f"Original shape: {report['original_shape']}")
    print(f"Cleaned shape: {report['cleaned_shape']}")
    print(f"Rows removed: {report['rows_removed']}")
    print(f"Columns removed: {report['columns_removed']}")
    print(f"Missing values before: {report['missing_values_before']}")
    print(f"Missing values after: {report['missing_values_after']}")
    print(f"Duplicate rows removed: {report['duplicate_rows_removed']}")

    return df, report
