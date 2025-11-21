"""
Upload CSV data to Snowflake table temp.echung.login_urls_activation_emails_accounts
Filters out error rows and replaces existing data.
"""
from snowflake_connector import SnowflakeConnection
import csv
import os
import argparse

# Configuration
ACCOUNT = "snowhouse"
USER = "echung"
DATABASE = "temp"
SCHEMA = "echung2"
BATCH_SIZE = 1000


def read_and_filter_csv(csv_filename):
    """
    Read CSV file and filter out rows with ERROR in classic_ui_url.
    
    Args:
        csv_filename: Path to CSV file
        
    Returns:
        Tuple of (filtered_rows, total_count, error_count)
    """
    if not os.path.exists(csv_filename):
        raise FileNotFoundError(f"CSV file not found: {csv_filename}")
    
    filtered_rows = []
    total_count = 0
    error_count = 0
    
    with open(csv_filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            total_count += 1
            classic_ui_url = row.get('classic_ui_url', '')
            
            # Filter out rows with ERROR (case-insensitive)
            if 'ERROR' in classic_ui_url.upper():
                error_count += 1
                continue
            
            # Skip empty rows
            if not row.get('account_name') or not classic_ui_url:
                error_count += 1
                continue
                
            filtered_rows.append(row)
    
    return filtered_rows, total_count, error_count


def create_table(sf, database, schema, table):
    """
    Create the table if it doesn't exist.
    
    Args:
        sf: SnowflakeConnection instance
        database: Database name
        schema: Schema name
        table: Table name
    """
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
        account_name VARCHAR(256),
        deployment VARCHAR(256),
        classic_ui_url VARCHAR(512)
    )
    """
    
    print(f"\n--- Creating table if not exists: {database}.{schema}.{table} ---")
    sf.execute_query(create_query, fetch=False)
    print(f"✓ Table ready")


def truncate_table(sf, database, schema, table):
    """
    Truncate the table to remove existing data.
    
    Args:
        sf: SnowflakeConnection instance
        database: Database name
        schema: Schema name
        table: Table name
    """
    truncate_query = f"TRUNCATE TABLE {database}.{schema}.{table}"
    
    print(f"\n--- Truncating table: {database}.{schema}.{table} ---")
    sf.execute_query(truncate_query, fetch=False)
    print(f"✓ Table truncated")


def batch_insert_rows(sf, database, schema, table, rows, batch_size):
    """
    Insert rows in batches using parameterized queries.
    
    Args:
        sf: SnowflakeConnection instance
        database: Database name
        schema: Schema name
        table: Table name
        rows: List of row dictionaries
        batch_size: Number of rows per batch
        
    Returns:
        Number of rows inserted
    """
    total_inserted = 0
    total_rows = len(rows)
    
    print(f"\n--- Inserting {total_rows} rows in batches of {batch_size} ---")
    
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        
        # Build INSERT statement with value placeholders
        insert_query = f"""
        INSERT INTO {database}.{schema}.{table} 
        (account_name, deployment, classic_ui_url)
        VALUES (%s, %s, %s)
        """
        
        # Prepare batch data as list of tuples
        batch_data = [
            (row['account_name'], row['deployment'], row['classic_ui_url'])
            for row in batch
        ]
        
        # Execute batch insert
        sf.cursor.executemany(insert_query, batch_data)
        
        total_inserted += len(batch)
        print(f"  Inserted batch: {total_inserted}/{total_rows} rows")
    
    return total_inserted


def main(csv_file, table):
    """
    Main function to upload CSV data to Snowflake.
    
    Args:
        csv_file: Path to CSV file to upload
        table: Table name in Snowflake
    """
    print("=" * 60)
    print("CSV Upload to Snowflake")
    print("=" * 60)
    
    # Read and filter CSV
    print(f"\n--- Reading CSV file: {csv_file} ---")
    try:
        filtered_rows, total_count, error_count = read_and_filter_csv(csv_file)
        print(f"✓ CSV read successfully")
        print(f"  Total rows in CSV: {total_count}")
        print(f"  Rows filtered out (errors/empty): {error_count}")
        print(f"  Rows to insert: {len(filtered_rows)}")
    except FileNotFoundError as e:
        print(f"✗ Error: {e}")
        return
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return
    
    if not filtered_rows:
        print("\n✗ No valid rows to insert. Exiting.")
        return
    
    # Connect to Snowflake
    sf = SnowflakeConnection(
        account=ACCOUNT,
        user=USER,
        database=DATABASE,
        schema=SCHEMA
    )
    
    try:
        # Connect (will open browser for authentication)
        sf.connect()
        
        # Verify connection
        result = sf.execute_query("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER()")
        if result:
            print(f"  Database: {result[0]['CURRENT_DATABASE()']}")
            print(f"  Schema: {result[0]['CURRENT_SCHEMA()']}")
            print(f"  User: {result[0]['CURRENT_USER()']}")
        
        # Create table if not exists
        create_table(sf, DATABASE, SCHEMA, table)
        
        # Truncate existing data
        truncate_table(sf, DATABASE, SCHEMA, table)
        
        # Insert filtered rows in batches
        inserted_count = batch_insert_rows(sf, DATABASE, SCHEMA, table, filtered_rows, BATCH_SIZE)
        
        # Verify insert
        verify_query = f"SELECT COUNT(*) as count FROM {DATABASE}.{SCHEMA}.{table}"
        result = sf.execute_query(verify_query)
        actual_count = result[0]['COUNT'] if result else 0
        
        print(f"\n{'='*60}")
        print(f"✓ Upload Complete!")
        print(f"{'='*60}")
        print(f"  Rows inserted: {inserted_count}")
        print(f"  Rows in table: {actual_count}")
        print(f"  Table: {DATABASE}.{SCHEMA}.{table}")
        
        if inserted_count != actual_count:
            print(f"\n⚠ Warning: Inserted count doesn't match table count!")
        
    except Exception as e:
        print(f"\n✗ Error during upload: {e}")
        raise
    
    finally:
        # Always close the connection
        sf.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Upload CSV data to Snowflake table, filtering out error rows'
    )
    parser.add_argument(
        '--csv',
        type=str,
        required=True,
        help='Path to CSV file to upload'
    )
    parser.add_argument(
        '--table',
        type=str,
        required=True,
        help='Table name in Snowflake (without database/schema prefix)'
    )
    
    args = parser.parse_args()
    main(args.csv, args.table)

