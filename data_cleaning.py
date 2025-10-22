#!/usr/bin/env python3

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Union
from scipy import stats

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Comprehensive data cleaning class with multiple cleaning strategies.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize DataCleaner with optional configuration.
        
        Args:
            config: Dictionary containing cleaning options
        """
        self.config = config or {}
        self.cleaning_report = {
            'missing_values_handled': 0,
            'duplicates_removed': 0,
            'outliers_detected': 0,
            'types_corrected': 0
        }
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all cleaning operations to a DataFrame.
        
        Args:
            df: Input DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Starting data cleaning on DataFrame with shape {df.shape}")
        df_cleaned = df.copy()
        
        # Remove duplicates
        if self.config.get('remove_duplicates', True):
            df_cleaned = self.remove_duplicates(df_cleaned)
        
        # Handle missing values
        if self.config.get('handle_missing_values', True):
            df_cleaned = self.handle_missing_values(df_cleaned)
        
        # Detect and handle outliers
        if self.config.get('outlier_detection_method'):
            df_cleaned = self.detect_and_handle_outliers(df_cleaned)
        
        # Correct data types
        df_cleaned = self.correct_data_types(df_cleaned)
        
        # Standardize categorical data
        if self.config.get('standardize_categorical', True):
            df_cleaned = self.standardize_categorical(df_cleaned)
        
        logger.info(f"Cleaning completed. Final shape: {df_cleaned.shape}")
        self._log_cleaning_report()
        
        return df_cleaned
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame without duplicates
        """
        initial_count = len(df)
        df_cleaned = df.drop_duplicates()
        duplicates_removed = initial_count - len(df_cleaned)
        
        self.cleaning_report['duplicates_removed'] = duplicates_removed
        logger.info(f"Removed {duplicates_removed} duplicate rows")
        
        return df_cleaned
    
    def handle_missing_values(self, df: pd.DataFrame, 
                             strategy: str = 'auto') -> pd.DataFrame:
        """
        Handle missing values using various strategies.
        
        Args:
            df: Input DataFrame
            strategy: Strategy to use ('auto', 'drop', 'mean', 'median', 'mode')
            
        Returns:
            DataFrame with handled missing values
        """
        df_cleaned = df.copy()
        missing_count = df.isnull().sum().sum()
        
        if missing_count == 0:
            logger.info("No missing values found")
            return df_cleaned
        
        logger.info(f"Handling {missing_count} missing values")
        
        for column in df_cleaned.columns:
            missing_in_col = df_cleaned[column].isnull().sum()
            
            if missing_in_col == 0:
                continue
            
            # Determine strategy
            if strategy == 'auto':
                # Auto-select based on data type and missing percentage
                missing_pct = missing_in_col / len(df_cleaned)
                
                if missing_pct > 0.5:
                    # Drop column if more than 50% missing
                    df_cleaned = df_cleaned.drop(columns=[column])
                    logger.info(f"Dropped column '{column}' ({missing_pct:.1%} missing)")
                    continue
                
                if pd.api.types.is_numeric_dtype(df_cleaned[column]):
                    # Use median for numerical data
                    df_cleaned[column].fillna(df_cleaned[column].median(), inplace=True)
                    logger.info(f"Filled '{column}' with median")
                else:
                    # Use mode for categorical data
                    mode_value = df_cleaned[column].mode()
                    if len(mode_value) > 0:
                        df_cleaned[column].fillna(mode_value[0], inplace=True)
                        logger.info(f"Filled '{column}' with mode")
            
            elif strategy == 'drop':
                df_cleaned = df_cleaned.dropna(subset=[column])
            elif strategy == 'mean' and pd.api.types.is_numeric_dtype(df_cleaned[column]):
                df_cleaned[column].fillna(df_cleaned[column].mean(), inplace=True)
            elif strategy == 'median' and pd.api.types.is_numeric_dtype(df_cleaned[column]):
                df_cleaned[column].fillna(df_cleaned[column].median(), inplace=True)
            elif strategy == 'mode':
                mode_value = df_cleaned[column].mode()
                if len(mode_value) > 0:
                    df_cleaned[column].fillna(mode_value[0], inplace=True)
        
        self.cleaning_report['missing_values_handled'] = missing_count
        return df_cleaned
    
    def detect_and_handle_outliers(self, df: pd.DataFrame, 
                                   method: Optional[str] = None,
                                   threshold: float = 1.5,
                                   action: str = 'cap') -> pd.DataFrame:
        """
        Detect and handle outliers in numerical columns.
        
        Args:
            df: Input DataFrame
            method: Detection method ('IQR' or 'zscore')
            threshold: Threshold for outlier detection
            action: Action to take ('cap', 'remove', or 'flag')
            
        Returns:
            DataFrame with handled outliers
        """
        df_cleaned = df.copy()
        method = method or self.config.get('outlier_detection_method', 'IQR')
        threshold = self.config.get('outlier_threshold', threshold)
        
        numerical_cols = df_cleaned.select_dtypes(include=[np.number]).columns
        outlier_count = 0
        
        for column in numerical_cols:
            if method.upper() == 'IQR':
                # IQR method
                Q1 = df_cleaned[column].quantile(0.25)
                Q3 = df_cleaned[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                outliers = (df_cleaned[column] < lower_bound) | (df_cleaned[column] > upper_bound)
                outlier_count += outliers.sum()
                
                if action == 'cap':
                    df_cleaned.loc[df_cleaned[column] < lower_bound, column] = lower_bound
                    df_cleaned.loc[df_cleaned[column] > upper_bound, column] = upper_bound
                elif action == 'remove':
                    df_cleaned = df_cleaned[~outliers]
                elif action == 'flag':
                    df_cleaned[f'{column}_outlier'] = outliers
            
            elif method.upper() == 'ZSCORE':
                # Z-score method
                z_scores = np.abs(stats.zscore(df_cleaned[column].dropna()))
                outliers = z_scores > threshold
                outlier_count += outliers.sum()
                
                if action == 'cap':
                    mean_val = df_cleaned[column].mean()
                    std_val = df_cleaned[column].std()
                    lower_bound = mean_val - threshold * std_val
                    upper_bound = mean_val + threshold * std_val
                    df_cleaned.loc[df_cleaned[column] < lower_bound, column] = lower_bound
                    df_cleaned.loc[df_cleaned[column] > upper_bound, column] = upper_bound
                elif action == 'remove':
                    df_cleaned = df_cleaned[z_scores <= threshold]
                elif action == 'flag':
                    df_cleaned[f'{column}_outlier'] = z_scores > threshold
        
        self.cleaning_report['outliers_detected'] = outlier_count
        logger.info(f"Detected and handled {outlier_count} outliers using {method} method")
        
        return df_cleaned
    
    def correct_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Automatically correct data types where possible.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with corrected types
        """
        df_cleaned = df.copy()
        corrections = 0
        
        for column in df_cleaned.columns:
            # Try to convert to numeric
            if df_cleaned[column].dtype == 'object':
                try:
                    # Try converting to numeric
                    converted = pd.to_numeric(df_cleaned[column], errors='coerce')
                    if converted.notna().sum() / len(converted) > 0.8:  # If >80% successful
                        df_cleaned[column] = converted
                        corrections += 1
                        logger.info(f"Converted '{column}' to numeric type")
                        continue
                except:
                    pass
                
                # Try converting to datetime
                try:
                    converted = pd.to_datetime(df_cleaned[column], errors='coerce')
                    if converted.notna().sum() / len(converted) > 0.8:
                        df_cleaned[column] = converted
                        corrections += 1
                        logger.info(f"Converted '{column}' to datetime type")
                        continue
                except:
                    pass
        
        self.cleaning_report['types_corrected'] = corrections
        return df_cleaned
    
    def standardize_categorical(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize categorical data (text normalization, whitespace handling).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized categorical data
        """
        df_cleaned = df.copy()
        categorical_cols = df_cleaned.select_dtypes(include=['object']).columns
        
        for column in categorical_cols:
            # Remove leading/trailing whitespace
            df_cleaned[column] = df_cleaned[column].str.strip()
            
            # Normalize case (lowercase)
            df_cleaned[column] = df_cleaned[column].str.lower()
            
            # Replace multiple spaces with single space
            df_cleaned[column] = df_cleaned[column].str.replace(r'\s+', ' ', regex=True)
            
            logger.debug(f"Standardized categorical column: {column}")
        
        return df_cleaned
    
    def _log_cleaning_report(self):
        """Log the cleaning report summary."""
        logger.info("=" * 50)
        logger.info("DATA CLEANING REPORT")
        logger.info("=" * 50)
        for key, value in self.cleaning_report.items():
            logger.info(f"{key.replace('_', ' ').title()}: {value}")
        logger.info("=" * 50)
    
    def get_cleaning_report(self) -> Dict[str, int]:
        """
        Get the cleaning report.
        
        Returns:
            Dictionary with cleaning statistics
        """
        return self.cleaning_report.copy()

