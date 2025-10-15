#!/usr/bin/env python3
"""
Data Cleaning and Standardization Pipeline for Thesis Project
Tutor2 - Data preprocessing pipeline using PostgreSQL
"""

import argparse
import json
import os
import sys
from pathlib import Path
import pandas as pd

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent / "utils"))

from database import DatabaseConnection, get_db_connection
from data_cleaning import clean_thesis_data


def load_config(config_path: str = "config/database_config.json") -> dict:
    """Load configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Configuration file not found: {config_path}")
        print("Please copy config/database_config.json.example to config/database_config.json and update the values.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error reading configuration file: {e}")
        sys.exit(1)


def setup_database_tables(db: DatabaseConnection, config: dict) -> bool:
    """Create necessary database tables for the thesis project"""
    tables_created = 0

    # Create raw_data table
    raw_columns = {
        'id': 'SERIAL PRIMARY KEY',
        'data_source': 'VARCHAR(255)',
        'collection_date': 'TIMESTAMP',
        'raw_content': 'TEXT',
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }

    if db.create_table('raw_data', raw_columns):
        print("✓ Created raw_data table")
        tables_created += 1
    else:
        print("✗ Failed to create raw_data table")

    # Create cleaned_data table
    cleaned_columns = {
        'id': 'SERIAL PRIMARY KEY',
        'raw_data_id': 'INTEGER REFERENCES raw_data(id)',
        'cleaned_content': 'JSONB',
        'cleaning_report': 'JSONB',
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }

    if db.create_table('cleaned_data', cleaned_columns):
        print("✓ Created cleaned_data table")
        tables_created += 1
    else:
        print("✗ Failed to create cleaned_data table")

    return tables_created == 2


def load_data_to_database(db: DatabaseConnection, file_path: str, config: dict) -> bool:
    """Load raw data into PostgreSQL database"""
    try:
        # Load data using pandas
        df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)

        print(f"Loading {len(df)} rows from {file_path}")

        # Insert data into raw_data table
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO raw_data (data_source, collection_date, raw_content)
            VALUES (%s, %s, %s)
            """

            db.execute_update(insert_query, (
                file_path,
                pd.Timestamp.now(),
                row.to_json()
            ))

        print("✓ Raw data loaded into database")
        return True

    except Exception as e:
        print(f"Error loading data to database: {e}")
        return False


def process_and_store_cleaned_data(db: DatabaseConnection, config: dict) -> bool:
    """Process raw data and store cleaned results"""
    try:
        # Get raw data from database
        raw_data_query = "SELECT id, raw_content FROM raw_data"
        raw_results = db.execute_query(raw_data_query)

        if not raw_results:
            print("No raw data found in database")
            return False

        print(f"Processing {len(raw_results)} raw data records")

        for raw_id, raw_content_json in raw_results:
            try:
                # Convert JSON back to DataFrame row
                row_data = json.loads(raw_content_json)

                # For now, we'll process each record individually
                # In a real scenario, you'd want to batch process this
                print(f"Processing record {raw_id}")

                # Here you would apply your thesis-specific cleaning logic
                # For now, we'll just mark it as processed

                # Store cleaned data
                cleaned_query = """
                INSERT INTO cleaned_data (raw_data_id, cleaned_content, cleaning_report)
                VALUES (%s, %s, %s)
                """

                db.execute_update(cleaned_query, (
                    raw_id,
                    json.dumps({"status": "processed", "record_id": raw_id}),
                    json.dumps({"method": "basic_processing", "timestamp": str(pd.Timestamp.now())})
                ))

        print("✓ Cleaned data processed and stored")
        return True

    except Exception as e:
        print(f"Error processing cleaned data: {e}")
        return False


def export_cleaned_data(db: DatabaseConnection, output_path: str, config: dict) -> bool:
    """Export cleaned data to file"""
    try:
        # Get cleaned data from database
        query = """
        SELECT cd.id, cd.cleaned_content, cd.cleaning_report, rd.data_source
        FROM cleaned_data cd
        JOIN raw_data rd ON cd.raw_data_id = rd.id
        """

        results = db.execute_query(query)

        if not results:
            print("No cleaned data found in database")
            return False

        # Convert to DataFrame for export
        rows = []
        for row in results:
            cleaned_content = json.loads(row[1])
            cleaning_report = json.loads(row[2])
            data_source = row[3]

            row_data = {
                'cleaned_data_id': row[0],
                'data_source': data_source,
                'cleaning_report': cleaning_report,
                **cleaned_content
            }
            rows.append(row_data)

        df = pd.DataFrame(rows)

        # Save to file
        if output_path.endswith('.csv'):
            df.to_csv(output_path, index=False)
        elif output_path.endswith('.xlsx'):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)

        print(f"✓ Cleaned data exported to {output_path}")
        return True

    except Exception as e:
        print(f"Error exporting cleaned data: {e}")
        return False


def main():
    """Main pipeline function"""
    parser = argparse.ArgumentParser(
        description='Data Cleaning and Standardization Pipeline for Thesis Project',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python scripts/data_pipeline.py --load data/thesis_data.csv
  python scripts/data_pipeline.py --process
  python scripts/data_pipeline.py --export data/cleaned_data.csv
  python scripts/data_pipeline.py --full data/thesis_data.csv data/cleaned_output.csv
        '''
    )

    parser.add_argument('--load', metavar='FILE', help='Load raw data from file to database')
    parser.add_argument('--process', action='store_true', help='Process and clean data in database')
    parser.add_argument('--export', metavar='FILE', help='Export cleaned data from database to file')
    parser.add_argument('--full', nargs=2, metavar=('INPUT_FILE', 'OUTPUT_FILE'),
                       help='Run full pipeline: load, process, and export')

    args = parser.parse_args()

    # Load configuration
    config = load_config()

    # Get database connection
    db = get_db_connection()
    if not db:
        sys.exit(1)

    try:
        if args.full:
            input_file, output_file = args.full

            print("=== Starting Full Pipeline ===")

            # Setup database tables
            print("Setting up database tables...")
            if not setup_database_tables(db, config):
                sys.exit(1)

            # Load data
            print("Loading data...")
            if not load_data_to_database(db, input_file, config):
                sys.exit(1)

            # Process data
            print("Processing data...")
            if not process_and_store_cleaned_data(db, config):
                sys.exit(1)

            # Export data
            print("Exporting cleaned data...")
            if not export_cleaned_data(db, output_file, config):
                sys.exit(1)

            print("✓ Full pipeline completed successfully!")

        elif args.load:
            print("=== Loading Data ===")
            if not setup_database_tables(db, config):
                sys.exit(1)
            if not load_data_to_database(db, args.load, config):
                sys.exit(1)

        elif args.process:
            print("=== Processing Data ===")
            if not process_and_store_cleaned_data(db, config):
                sys.exit(1)

        elif args.export:
            print("=== Exporting Data ===")
            if not export_cleaned_data(db, args.export, config):
                sys.exit(1)

        else:
            parser.print_help()

    finally:
        db.disconnect()


if __name__ == "__main__":
    main()
