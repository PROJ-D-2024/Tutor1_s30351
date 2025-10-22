#!/usr/bin/env python3
"""
Data Standardization Module
Implements various data standardization techniques including normalization,
scaling, and date/time formatting with reusable, modular functions.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataStandardizer:
    """
    Comprehensive data standardization class implementing various normalization
    and standardization techniques.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.scalers = {}  # Store fitted scalers for inverse transform
        self.encoders = {}  # Store fitted encoders
        self.standardization_report = {
            'columns_normalized': [],
            'columns_scaled': [],
            'columns_encoded': [],
            'dates_standardized': []
        }
    
    def standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all standardization operations to a DataFrame.
        
        Args:
            df: Input DataFrame to standardize
            
        Returns:
            Standardized DataFrame
        """
        logger.info(f"Starting data standardization on DataFrame with shape {df.shape}")
        df_standardized = df.copy()
        
        # Standardize date/time columns
        df_standardized = self.standardize_dates(df_standardized)
        
        # Normalize numerical data if configured
        if self.config.get('normalize_numerical', False):
            normalization_method = self.config.get('normalization_method', 'minmax')
            df_standardized = self.normalize_numerical_data(
                df_standardized, 
                method=normalization_method
            )
        
        # Encode categorical data if configured
        if self.config.get('encode_categorical', False):
            encoding_method = self.config.get('encoding_method', 'label')
            df_standardized = self.encode_categorical_data(
                df_standardized,
                method=encoding_method
            )
        
        logger.info(f"Standardization completed. Final shape: {df_standardized.shape}")
        self._log_standardization_report()
        
        return df_standardized
    
    def normalize_numerical_data(self, df: pd.DataFrame, 
                                 method: str = 'minmax',
                                 columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Normalize or scale numerical columns using various methods.
        
        Args:
            df: Input DataFrame
            method: Normalization method ('minmax', 'zscore', 'robust')
            columns: Specific columns to normalize (None for all numerical)
            
        Returns:
            DataFrame with normalized numerical columns
        """
        df_normalized = df.copy()
        
        # Select columns to normalize
        if columns is None:
            columns = df_normalized.select_dtypes(include=[np.number]).columns.tolist()
        
        if not columns:
            logger.warning("No numerical columns found for normalization")
            return df_normalized
        
        logger.info(f"Normalizing {len(columns)} numerical columns using {method} method")
        
        # Select appropriate scaler
        if method.lower() == 'minmax':
            scaler = MinMaxScaler()
            scaler_name = "Min-Max Scaling"
        elif method.lower() == 'zscore' or method.lower() == 'standard':
            scaler = StandardScaler()
            scaler_name = "Z-Score Normalization"
        elif method.lower() == 'robust':
            scaler = RobustScaler()
            scaler_name = "Robust Scaling"
        else:
            logger.error(f"Unknown normalization method: {method}")
            return df_normalized
        
        try:
            # Fit and transform the data
            df_normalized[columns] = scaler.fit_transform(df_normalized[columns])
            
            # Store the scaler for potential inverse transform
            self.scalers[f'{method}_scaler'] = scaler
            self.scalers[f'{method}_columns'] = columns
            
            self.standardization_report['columns_normalized'].extend(columns)
            logger.info(f"Applied {scaler_name} to columns: {', '.join(columns)}")
            
        except Exception as e:
            logger.error(f"Error during normalization: {e}")
        
        return df_normalized
    
    def minmax_scaling(self, df: pd.DataFrame, 
                      columns: Optional[List[str]] = None,
                      feature_range: Tuple[float, float] = (0, 1)) -> pd.DataFrame:
        """
        Apply Min-Max scaling to normalize data to a specific range.
        Formula: X_scaled = (X - X_min) / (X_max - X_min) * (max - min) + min
        """
        df_scaled = df.copy()
        
        if columns is None:
            columns = df_scaled.select_dtypes(include=[np.number]).columns.tolist()
        
        scaler = MinMaxScaler(feature_range=feature_range)
        df_scaled[columns] = scaler.fit_transform(df_scaled[columns])
        
        self.scalers['minmax'] = scaler
        logger.info(f"Applied Min-Max scaling to {len(columns)} columns")
        
        return df_scaled
    
    def zscore_normalization(self, df: pd.DataFrame,
                            columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Apply Z-score normalization (standardization).
        Formula: X_normalized = (X - mean) / std
        """
        df_normalized = df.copy()
        
        if columns is None:
            columns = df_normalized.select_dtypes(include=[np.number]).columns.tolist()
        
        scaler = StandardScaler()
        df_normalized[columns] = scaler.fit_transform(df_normalized[columns])
        
        self.scalers['zscore'] = scaler
        logger.info(f"Applied Z-score normalization to {len(columns)} columns")
        
        return df_normalized
    
    def robust_scaling(self, df: pd.DataFrame,
                      columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Apply scaling using median and IQR (robust to outliers).
        Formula: X_scaled = (X - median) / IQR
        """
        df_scaled = df.copy()
        
        if columns is None:
            columns = df_scaled.select_dtypes(include=[np.number]).columns.tolist()
        
        scaler = RobustScaler()
        df_scaled[columns] = scaler.fit_transform(df_scaled[columns])
        
        self.scalers['robust'] = scaler
        logger.info(f"Applied Robust scaling to {len(columns)} columns")
        
        return df_scaled
    
    def standardize_dates(self, df: pd.DataFrame, 
                         date_format: str = 'ISO',
                         columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Standardize date and time columns to a uniform format.
        """
        df_standardized = df.copy()
        
        # Auto-detect date columns if not specified
        if columns is None:
            columns = []
            for col in df_standardized.columns:
                if pd.api.types.is_datetime64_any_dtype(df_standardized[col]):
                    columns.append(col)
                elif df_standardized[col].dtype == 'object':
                    # Try to parse as date
                    try:
                        sample = df_standardized[col].dropna().iloc[0] if len(df_standardized[col].dropna()) > 0 else None
                        if sample and isinstance(sample, str):
                            pd.to_datetime(sample)
                            columns.append(col)
                    except:
                        continue
        
        if not columns:
            logger.info("No date columns detected")
            return df_standardized
        
        logger.info(f"Standardizing {len(columns)} date columns to {date_format} format")
        
        for col in columns:
            try:
                # Convert to datetime if not already
                if not pd.api.types.is_datetime64_any_dtype(df_standardized[col]):
                    df_standardized[col] = pd.to_datetime(df_standardized[col], errors='coerce')
                
                # Format based on specified format
                if date_format.upper() == 'ISO':
                    # ISO 8601 format: YYYY-MM-DD HH:MM:SS
                    df_standardized[col] = df_standardized[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif date_format.upper() == 'US':
                    # US format: MM/DD/YYYY
                    df_standardized[col] = df_standardized[col].dt.strftime('%m/%d/%Y')
                elif date_format.upper() == 'EU':
                    # European format: DD/MM/YYYY
                    df_standardized[col] = df_standardized[col].dt.strftime('%d/%m/%Y')
                
                self.standardization_report['dates_standardized'].append(col)
                logger.info(f"Standardized date column: {col}")
                
            except Exception as e:
                logger.warning(f"Could not standardize date column '{col}': {e}")
        
        return df_standardized
    
    def encode_categorical_data(self, df: pd.DataFrame,
                               method: str = 'label',
                               columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Encode categorical data using various methods.
        
        Args:
            df: Input DataFrame
            method: Encoding method ('label', 'onehot', 'ordinal')
            columns: Specific columns to encode (None for all categorical)
            
        Returns:
            DataFrame with encoded categorical columns
        """
        df_encoded = df.copy()
        
        # Select categorical columns if not specified
        if columns is None:
            columns = df_encoded.select_dtypes(include=['object', 'category']).columns.tolist()
        
        if not columns:
            logger.info("No categorical columns found for encoding")
            return df_encoded
        
        logger.info(f"Encoding {len(columns)} categorical columns using {method} method")
        
        for col in columns:
            try:
                if method.lower() == 'label':
                    # Label encoding: Convert categories to integers
                    le = LabelEncoder()
                    df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
                    self.encoders[col] = le
                    logger.info(f"Label encoded column: {col}")
                
                elif method.lower() == 'onehot':
                    # One-hot encoding: Create binary columns for each category
                    encoded = pd.get_dummies(df_encoded[col], prefix=col)
                    df_encoded = pd.concat([df_encoded, encoded], axis=1)
                    df_encoded = df_encoded.drop(columns=[col])
                    logger.info(f"One-hot encoded column: {col}")
                
                self.standardization_report['columns_encoded'].append(col)
                
            except Exception as e:
                logger.warning(f"Could not encode column '{col}': {e}")
        
        return df_encoded
    
    def inverse_transform_numerical(self, df: pd.DataFrame, 
                                   method: str = 'minmax') -> pd.DataFrame:
        """
        Inverse transform normalized/scaled data back to original scale.
        
        Args:
            df: DataFrame with normalized data
            method: Method used for normalization
            
        Returns:
            DataFrame with original scale
        """
        scaler_key = f'{method}_scaler'
        columns_key = f'{method}_columns'
        
        if scaler_key not in self.scalers or columns_key not in self.scalers:
            logger.error(f"No scaler found for method: {method}")
            return df
        
        df_original = df.copy()
        scaler = self.scalers[scaler_key]
        columns = self.scalers[columns_key]
        
        try:
            df_original[columns] = scaler.inverse_transform(df_original[columns])
            logger.info(f"Inverse transformed {len(columns)} columns")
        except Exception as e:
            logger.error(f"Error during inverse transform: {e}")
        
        return df_original
    
    def _log_standardization_report(self):
        """Log the standardization report summary."""
        logger.info("=" * 50)
        logger.info("DATA STANDARDIZATION REPORT")
        logger.info("=" * 50)
        
        if self.standardization_report['columns_normalized']:
            logger.info(f"Normalized Columns: {', '.join(self.standardization_report['columns_normalized'])}")
        
        if self.standardization_report['columns_scaled']:
            logger.info(f"Scaled Columns: {', '.join(self.standardization_report['columns_scaled'])}")
        
        if self.standardization_report['columns_encoded']:
            logger.info(f"Encoded Columns: {', '.join(self.standardization_report['columns_encoded'])}")
        
        if self.standardization_report['dates_standardized']:
            logger.info(f"Standardized Dates: {', '.join(self.standardization_report['dates_standardized'])}")
        
        logger.info("=" * 50)
    
    def get_standardization_report(self) -> Dict[str, List[str]]:
        return self.standardization_report.copy()
    
    def save_scalers(self, filepath: str):
        import pickle
        
        try:
            with open(filepath, 'wb') as f:
                pickle.dump({'scalers': self.scalers, 'encoders': self.encoders}, f)
            logger.info(f"Scalers saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving scalers: {e}")
    
    def load_scalers(self, filepath: str):
        import pickle
        
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
                self.scalers = data.get('scalers', {})
                self.encoders = data.get('encoders', {})
            logger.info(f"Scalers loaded from {filepath}")
        except Exception as e:
            logger.error(f"Error loading scalers: {e}")

