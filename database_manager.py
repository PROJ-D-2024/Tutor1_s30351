#!/usr/bin/env python3
"""
Database Manager Module
Handles all database interactions including connection management,
data insertion, updates, and retrievals with proper error handling.
"""

import json
import logging
from typing import Optional, Dict, Any, List
import psycopg2
from psycopg2 import pool, sql, Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages PostgreSQL database connections and operations.
    Implements connection pooling for efficient resource management.
    """
    
    def __init__(self, config_path: str = "config/database_config.json"):
        """
        Initialize DatabaseManager with configuration.
        
        Args:
            config_path: Path to the database configuration JSON file
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is malformed
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.connection_pool = None
        self._initialize_pool()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load database configuration from JSON file.
        
        Returns:
            Dictionary containing database configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is malformed
        """
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def _initialize_pool(self):
        """
        Initialize connection pool for efficient database connections.
        
        Raises:
            psycopg2.Error: If connection pool cannot be created
        """
        try:
            db_config = self.config['database']
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1,  # Minimum connections
                10,  # Maximum connections
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database_name'],
                user=db_config['username'],
                password=db_config['password']
            )
            if self.connection_pool:
                logger.info("Connection pool created successfully")
        except KeyError as e:
            logger.error(f"Missing configuration key: {e}")
            raise
        except Error as e:
            logger.error(f"Error creating connection pool: {e}")
            raise
    
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            Database connection object
            
        Raises:
            psycopg2.Error: If connection cannot be obtained
        """
        try:
            connection = self.connection_pool.getconn()
            if connection:
                logger.debug("Connection retrieved from pool")
                return connection
        except Error as e:
            logger.error(f"Error getting connection from pool: {e}")
            raise
    
    def return_connection(self, connection):
        """
        Return a connection to the pool.
        
        Args:
            connection: Database connection to return
        """
        if connection:
            self.connection_pool.putconn(connection)
            logger.debug("Connection returned to pool")
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("All database connections closed")
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, 
                                   drop_if_exists: bool = False) -> bool:
        """
        Create a database table from a pandas DataFrame schema.
        
        Args:
            df: DataFrame to use as schema template
            table_name: Name of the table to create
            drop_if_exists: Whether to drop existing table
            
        Returns:
            True if successful, False otherwise
        """
        connection = None
        cursor = None
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            # Drop table if requested
            if drop_if_exists:
                drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                    sql.Identifier(table_name)
                )
                cursor.execute(drop_query)
                logger.info(f"Dropped existing table: {table_name}")
            
            # Map pandas dtypes to PostgreSQL types
            type_mapping = {
                'int64': 'BIGINT',
                'float64': 'DOUBLE PRECISION',
                'object': 'TEXT',
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'TIMESTAMP',
                'timedelta[ns]': 'INTERVAL'
            }
            
            # Build CREATE TABLE query
            columns = []
            for col_name, dtype in df.dtypes.items():
                pg_type = type_mapping.get(str(dtype), 'TEXT')
                columns.append(f"{col_name} {pg_type}")
            
            create_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_query)
            connection.commit()
            
            logger.info(f"Table '{table_name}' created successfully")
            return True
            
        except Error as e:
            logger.error(f"Error creating table: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.return_connection(connection)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        batch_size: int = 1000) -> bool:
        """
        Insert DataFrame into database table efficiently using batch inserts.
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            batch_size: Number of rows to insert per batch
            
        Returns:
            True if successful, False otherwise
        """
        connection = None
        cursor = None
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            # Prepare column names
            columns = list(df.columns)
            placeholders = ','.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
            
            # Insert in batches
            total_rows = len(df)
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]
                
                # Convert DataFrame to list of tuples
                data = [tuple(row) for row in batch.values]
                
                # Execute batch insert
                cursor.executemany(insert_query, data)
                connection.commit()
                
                logger.info(f"Inserted rows {start_idx+1} to {end_idx} of {total_rows}")
            
            logger.info(f"Successfully inserted {total_rows} rows into {table_name}")
            return True
            
        except Error as e:
            logger.error(f"Error inserting data: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.return_connection(connection)
    
    def update_dataframe(self, df: pd.DataFrame, table_name: str, 
                        key_column: str) -> bool:
        """
        Update database records from DataFrame based on a key column.
        
        Args:
            df: DataFrame with updated data
            table_name: Target table name
            key_column: Column to use as update key
            
        Returns:
            True if successful, False otherwise
        """
        connection = None
        cursor = None
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            # Build UPDATE query
            columns = [col for col in df.columns if col != key_column]
            set_clause = ', '.join([f"{col} = %s" for col in columns])
            update_query = f"UPDATE {table_name} SET {set_clause} WHERE {key_column} = %s"
            
            # Prepare data
            update_count = 0
            for _, row in df.iterrows():
                values = [row[col] for col in columns] + [row[key_column]]
                cursor.execute(update_query, values)
                update_count += cursor.rowcount
            
            connection.commit()
            logger.info(f"Updated {update_count} rows in {table_name}")
            return True
            
        except Error as e:
            logger.error(f"Error updating data: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.return_connection(connection)
    
    def read_table(self, table_name: str, columns: Optional[List[str]] = None,
                   where_clause: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Read data from database table into DataFrame.
        
        Args:
            table_name: Table to read from
            columns: Specific columns to retrieve (None for all)
            where_clause: Optional WHERE clause for filtering
            
        Returns:
            DataFrame with query results or None if error
        """
        connection = None
        
        try:
            connection = self.get_connection()
            
            # Build query
            col_str = '*' if columns is None else ', '.join(columns)
            query = f"SELECT {col_str} FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            # Execute query and load into DataFrame
            df = pd.read_sql_query(query, connection)
            logger.info(f"Retrieved {len(df)} rows from {table_name}")
            return df
            
        except Error as e:
            logger.error(f"Error reading table: {e}")
            return None
        finally:
            if connection:
                self.return_connection(connection)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> bool:
        """
        Execute a custom SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters for safe parameterization
            
        Returns:
            True if successful, False otherwise
        """
        connection = None
        cursor = None
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            connection.commit()
            logger.info("Query executed successfully")
            return True
            
        except Error as e:
            logger.error(f"Error executing query: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.return_connection(connection)
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        connection = None
        cursor = None
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            exists = cursor.fetchone()[0]
            return exists
            
        except Error as e:
            logger.error(f"Error checking table existence: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.return_connection(connection)

