#!/usr/bin/env python3
"""
Data Pipeline Script
Automated ETL (Extract, Transform, Load) pipeline for data cleaning and standardization.
Integrates database operations with comprehensive error handling.
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from typing import Optional

# Add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from database_manager import DatabaseManager
from data_cleaning import DataCleaner
from data_standardization import DataStandardizer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Automated data pipeline for cleaning, standardizing, and loading data.
    """
    
    def __init__(self, config_path: str = "config/database_config.json"):
        """
        Initialize DataPipeline with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        try:
            self.db_manager = DatabaseManager(config_path)
            
            # Load configuration for cleaning and standardization
            import json
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            self.data_cleaner = DataCleaner(config.get('cleaning_options', {}))
            self.data_standardizer = DataStandardizer(config.get('standardization_options', {}))
            self.config = config
            
            logger.info("DataPipeline initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing DataPipeline: {e}")
            raise
    
    def extract_from_csv(self, filepath: str) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            filepath: Path to CSV file
            
        Returns:
            DataFrame with extracted data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            pd.errors.ParserError: If file cannot be parsed
        """
        try:
            logger.info(f"Extracting data from {filepath}")
            df = pd.read_csv(filepath)
            logger.info(f"Extracted {len(df)} rows and {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading CSV: {e}")
            raise
    
    def extract_from_database(self, table_name: str) -> pd.DataFrame:
        """
        Extract data from database table.
        
        Args:
            table_name: Name of the table to extract
            
        Returns:
            DataFrame with extracted data
        """
        try:
            logger.info(f"Extracting data from database table: {table_name}")
            df = self.db_manager.read_table(table_name)
            
            if df is None:
                raise ValueError(f"Failed to read table: {table_name}")
            
            logger.info(f"Extracted {len(df)} rows from database")
            return df
        except Exception as e:
            logger.error(f"Error extracting from database: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame, 
                      clean: bool = True, 
                      standardize: bool = True) -> pd.DataFrame:
        """
        Transform data by cleaning and standardizing.
        
        Args:
            df: Input DataFrame
            clean: Whether to apply cleaning
            standardize: Whether to apply standardization
            
        Returns:
            Transformed DataFrame
        """
        try:
            logger.info("Starting data transformation")
            df_transformed = df.copy()
            
            # Apply cleaning
            if clean:
                logger.info("Applying data cleaning")
                df_transformed = self.data_cleaner.clean_dataframe(df_transformed)
                
                # Log cleaning report
                cleaning_report = self.data_cleaner.get_cleaning_report()
                logger.info(f"Cleaning report: {cleaning_report}")
            
            # Apply standardization
            if standardize:
                logger.info("Applying data standardization")
                df_transformed = self.data_standardizer.standardize_dataframe(df_transformed)
                
                # Log standardization report
                std_report = self.data_standardizer.get_standardization_report()
                logger.info(f"Standardization report: {std_report}")
            
            logger.info("Data transformation completed successfully")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            raise
    
    def load_to_database(self, df: pd.DataFrame, 
                        table_name: str,
                        mode: str = 'replace') -> bool:
        """
        Load data to database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Load mode ('replace', 'append', 'update')
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"Loading data to database table: {table_name} (mode: {mode})")
            
            if mode == 'replace':
                # Drop and recreate table
                if not self.db_manager.create_table_from_dataframe(
                    df, table_name, drop_if_exists=True
                ):
                    raise ValueError("Failed to create table")
                
                # Insert data
                if not self.db_manager.insert_dataframe(df, table_name):
                    raise ValueError("Failed to insert data")
            
            elif mode == 'append':
                # Check if table exists
                if not self.db_manager.table_exists(table_name):
                    # Create table if doesn't exist
                    if not self.db_manager.create_table_from_dataframe(df, table_name):
                        raise ValueError("Failed to create table")
                
                # Insert data
                if not self.db_manager.insert_dataframe(df, table_name):
                    raise ValueError("Failed to insert data")
            
            elif mode == 'update':
                # Update existing records (requires 'id' column)
                if 'id' not in df.columns:
                    raise ValueError("Update mode requires 'id' column")
                
                if not self.db_manager.update_dataframe(df, table_name, 'id'):
                    raise ValueError("Failed to update data")
            
            logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to database: {e}")
            return False
    
    def load_to_csv(self, df: pd.DataFrame, filepath: str) -> bool:
        """
        Load data to CSV file.
        
        Args:
            df: DataFrame to save
            filepath: Target file path
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"Saving data to CSV: {filepath}")
            
            # Create directory if doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            df.to_csv(filepath, index=False)
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return False
    
    def run_full_pipeline(self, input_path: str, 
                         output_path: Optional[str] = None,
                         output_table: Optional[str] = None,
                         input_type: str = 'csv',
                         output_type: str = 'both') -> bool:
        """
        Run the complete ETL pipeline.
        
        Args:
            input_path: Input file path or table name
            output_path: Output CSV file path
            output_table: Output database table name
            input_type: Input type ('csv' or 'database')
            output_type: Output type ('csv', 'database', or 'both')
            
        Returns:
            True if successful
        """
        try:
            logger.info("=" * 70)
            logger.info("STARTING FULL DATA PIPELINE")
            logger.info("=" * 70)
            
            # EXTRACT
            if input_type == 'csv':
                df = self.extract_from_csv(input_path)
            elif input_type == 'database':
                df = self.extract_from_database(input_path)
            else:
                raise ValueError(f"Invalid input type: {input_type}")
            
            # TRANSFORM
            df_transformed = self.transform_data(df, clean=True, standardize=True)
            
            # LOAD
            success = True
            
            if output_type in ['csv', 'both'] and output_path:
                if not self.load_to_csv(df_transformed, output_path):
                    success = False
            
            if output_type in ['database', 'both'] and output_table:
                if not self.load_to_database(df_transformed, output_table, mode='replace'):
                    success = False
            
            if success:
                logger.info("=" * 70)
                logger.info("PIPELINE COMPLETED SUCCESSFULLY")
                logger.info("=" * 70)
            else:
                logger.warning("Pipeline completed with some errors")
            
            return success
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False
        finally:
            # Cleanup
            self.db_manager.close_all_connections()


def main():
    """Main entry point for the data pipeline script."""
    parser = argparse.ArgumentParser(
        description='Automated data cleaning and standardization pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Load CSV to database
  python scripts/data_pipeline.py --load data/raw/input.csv --table raw_data
  
  # Process data from database table
  python scripts/data_pipeline.py --process raw_data --output processed_data
  
  # Export table to CSV
  python scripts/data_pipeline.py --export processed_data --csv data/output.csv
  
  # Run full pipeline
  python scripts/data_pipeline.py --full data/input.csv --csv data/output.csv --table cleaned_data
        '''
    )
    
    # Command modes
    parser.add_argument('--load', metavar='FILE', help='Load CSV file to database')
    parser.add_argument('--process', metavar='TABLE', help='Process and clean database table')
    parser.add_argument('--export', metavar='TABLE', help='Export database table to CSV')
    parser.add_argument('--full', metavar='FILE', help='Run full pipeline on CSV file')
    
    # Options
    parser.add_argument('--table', metavar='NAME', help='Database table name')
    parser.add_argument('--csv', metavar='FILE', help='Output CSV file path')
    parser.add_argument('--config', default='config/database_config.json', 
                       help='Configuration file path')
    parser.add_argument('--no-clean', action='store_true', help='Skip data cleaning')
    parser.add_argument('--no-standardize', action='store_true', help='Skip standardization')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    try:
        pipeline = DataPipeline(args.config)
    except Exception as e:
        logger.error(f"Failed to initialize pipeline: {e}")
        sys.exit(1)
    
    try:
        # Execute requested operation
        if args.load:
            # Load CSV to database
            if not args.table:
                logger.error("--table required for --load")
                sys.exit(1)
            
            df = pipeline.extract_from_csv(args.load)
            if pipeline.load_to_database(df, args.table, mode='replace'):
                logger.info("Load completed successfully")
            else:
                logger.error("Load failed")
                sys.exit(1)
        
        elif args.process:
            # Process data from database
            df = pipeline.extract_from_database(args.process)
            df_transformed = pipeline.transform_data(
                df, 
                clean=not args.no_clean,
                standardize=not args.no_standardize
            )
            
            # Save results
            output_table = args.table or f"{args.process}_processed"
            if pipeline.load_to_database(df_transformed, output_table, mode='replace'):
                logger.info("Processing completed successfully")
            else:
                logger.error("Processing failed")
                sys.exit(1)
        
        elif args.export:
            # Export database table to CSV
            if not args.csv:
                logger.error("--csv required for --export")
                sys.exit(1)
            
            df = pipeline.extract_from_database(args.export)
            if pipeline.load_to_csv(df, args.csv):
                logger.info("Export completed successfully")
            else:
                logger.error("Export failed")
                sys.exit(1)
        
        elif args.full:
            # Run full pipeline
            output_csv = args.csv or 'data/cleaned/output.csv'
            output_table = args.table or 'cleaned_data'
            
            if pipeline.run_full_pipeline(
                args.full, 
                output_path=output_csv,
                output_table=output_table,
                input_type='csv',
                output_type='both'
            ):
                logger.info("Full pipeline completed successfully")
            else:
                logger.error("Full pipeline failed")
                sys.exit(1)
        
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        pipeline.db_manager.close_all_connections()


if __name__ == "__main__":
    main()

