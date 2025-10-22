"""
Database connection utilities for PostgreSQL
"""

import psycopg2
import json
import os
from typing import Optional, Dict, Any


class DatabaseConnection:
    """PostgreSQL database connection manager"""

    def __init__(self, config_path: str = "config/database_config.json"):
        """Initialize database connection with configuration file"""
        self.config_path = config_path
        self.connection = None
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load database configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Configuration file not found: {self.config_path}")
            print("Please copy config/database_config.json.example to config/database_config.json and update the values.")
            raise
        except json.JSONDecodeError as e:
            print(f"Error reading configuration file: {e}")
            raise

    def connect(self) -> bool:
        """Establish connection to PostgreSQL database"""
        try:
            db_config = self.config['database']
            self.connection = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database_name'],
                user=db_config['username'],
                password=db_config['password']
            )
            self.connection.autocommit = False
            print(f"Successfully connected to database: {db_config['database_name']}")
            return True
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def get_connection(self):
        """Get the database connection object"""
        return self.connection

    def execute_query(self, query: str, params: tuple = None) -> Optional[list]:
        """Execute a SELECT query and return results"""
        if not self.connection:
            print("No active database connection.")
            return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or [])
            results = cursor.fetchall()
            cursor.close()
            return results
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")
            return None

    def execute_update(self, query: str, params: tuple = None) -> bool:
        """Execute an INSERT, UPDATE, or DELETE query"""
        if not self.connection:
            print("No active database connection.")
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or [])
            self.connection.commit()
            cursor.close()
            return True
        except psycopg2.Error as e:
            print(f"Error executing update: {e}")
            self.connection.rollback()
            return False

    def create_table(self, table_name: str, columns: Dict[str, str]) -> bool:
        """Create a new table with specified columns"""
        if not self.connection:
            print("No active database connection.")
            return False

        # Build CREATE TABLE query
        columns_str = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"

        return self.execute_update(query)

    def get_table_columns(self, table_name: str) -> Optional[list]:
        """Get column information for a table"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        return self.execute_query(query, (table_name,))

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = %s
        )
        """
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else False


def get_db_connection(config_path: str = "config/database_config.json") -> Optional[DatabaseConnection]:
    """Get a database connection instance"""
    db = DatabaseConnection(config_path)
    if db.connect():
        return db
    return None
