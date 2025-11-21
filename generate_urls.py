"""
Simple example of connecting to Snowflake with external browser authentication.
"""
from snowflake_connector import SnowflakeConnection
import csv
from datetime import datetime
import os
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Generate Snowflake login URLs')
parser.add_argument('--skip-processed-check', action='store_true',
                    help='Skip checking already processed accounts')
parser.add_argument('--query-file', type=str,
                    help='Path to file containing custom SQL query')
args = parser.parse_args()

# Read already processed accounts from existing CSV (if not skipped)
EXISTING_CSV = "/Users/echung/Developments/login-url-generator/snowflake_urls_combined_20251120_083535.csv"
processed_accounts = set()

if not args.skip_processed_check:
    if os.path.exists(EXISTING_CSV):
        print(f"Loading already processed accounts from {EXISTING_CSV}...")
        with open(EXISTING_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_accounts.add(row['account_name'])
        print(f"✓ Found {len(processed_accounts)} already processed accounts")
    else:
        print(f"No existing CSV found, will process all accounts")
else:
    print("Skipping processed accounts check (--skip-processed-check flag set)")

# ===== Connection 1: Snowhouse (to get account info) =====
print("=" * 60)
print("CONNECTION 1: Snowhouse - Getting Account Info")
print("=" * 60)

ACCOUNT1 = "snowhouse"  # Account identifier from https://snowhouse.snowflakecomputing.com/
USER1 = "echung"

sf1 = SnowflakeConnection(
    account=ACCOUNT1,
    user=USER1
)

try:
    # This will open a browser for authentication
    sf1.connect()
    
    # Test the connection
    result = sf1.execute_query("SELECT CURRENT_USER(), CURRENT_ROLE()")
    print("Connected as:", result[0])
    
    # Query to get account information for active accounts
    print("\n--- Querying Account Information from Snowhouse ---")
    
    # Load query from file if specified, otherwise use default
    if args.query_file:
        print(f"Loading custom query from: {args.query_file}")
        with open(args.query_file, 'r') as f:
            query = f.read()
    else:
        query = """
    SELECT 
        a.name as account_name,
        a.deployment,
        drc.snowflake_region,
        drc.snowflake_region_group || '.' || drc.snowflake_region as classic_ui_url
    FROM SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a
    LEFT JOIN snowhouse_import.public.deployment_region_cloud drc 
        ON a.deployment = drc.deployment
    WHERE a.SUSPENDED_TIME IS NULL  -- Not suspended
        AND a.DELETED_ON IS NULL  -- Not deleted
    """
    
    all_results = sf1.execute_query(query)
    
    # Filter out already processed accounts (if check was not skipped)
    if all_results:
        print(f"\nFound {len(all_results)} total account(s) from database")
        
        if not args.skip_processed_check and processed_accounts:
            results = [row for row in all_results if row['ACCOUNT_NAME'] not in processed_accounts]
            print(f"After filtering, {len(results)} account(s) remain to process")
        else:
            results = all_results
            print(f"Processing all {len(results)} account(s) (no filtering applied)")
        
        if results:
            print("\nSample of accounts to process:")
            for i, row in enumerate(results[:5]):
                print(f"  {i+1}. Account Name: {row['ACCOUNT_NAME']}")
                print(f"     Deployment: {row['DEPLOYMENT']}")
    else:
        print("No results found!")
        results = []
    
finally:
    sf1.close()


# ===== Connection 2: External Zone (to run the function) =====
print("\n" + "=" * 60)
print("CONNECTION 2: External Zone - Running URL Generator Function")
print("=" * 60)

ACCOUNT2 = "snowflake.prod1.us-west-2.external-zone.snowflakecomputing.com:8085"
USER2 = "echung"

sf2 = SnowflakeConnection(
    account=ACCOUNT2,
    user=USER2
)

try:
    # This will open a browser for authentication
    sf2.connect()
    
    # Test the connection
    result = sf2.execute_query("SELECT CURRENT_USER(), CURRENT_ROLE()")
    print("Connected as:", result[0])
    
    # Run the SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL query using parameters from snowhouse
    if results:
        # Create CSV file and write header
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"snowflake_urls_{timestamp}.csv"
        
        csvfile = open(csv_filename, 'w', newline='')
        fieldnames = ['account_name', 'deployment', 'classic_ui_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        csvfile.flush()  # Ensure header is written immediately
        
        print(f"\n✓ Writing results to: {csv_filename}")
        print("\n--- Running SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL ---")
        
        processed_count = 0
        
        for row in results:
            account_name = row['ACCOUNT_NAME']
            # Filter out "\\" prefix if found
            if account_name.startswith('\\\\'):
                account_name = account_name[2:]
            deployment = row['DEPLOYMENT']
            classic_ui_url = row['CLASSIC_UI_URL']
            
            print(f"\nGenerating URL for account: {account_name}")
            print(f"Using region parameter: {classic_ui_url}")
            
            query = f"SELECT SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL('{account_name}', '{classic_ui_url}')"
            try:
                result = sf2.execute_query(query)
                url = result[0][f"SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL('{account_name}', '{classic_ui_url}')"]
                print(f"✓ Generated URL: {url}")
                
                # Write to CSV immediately
                writer.writerow({
                    'account_name': account_name,
                    'deployment': deployment,
                    'classic_ui_url': url
                })
                csvfile.flush()  # Ensure row is written immediately
                processed_count += 1
                
            except Exception as e:
                print(f"✗ Error: {str(e)}")
                # Write error entry to CSV immediately
                writer.writerow({
                    'account_name': account_name,
                    'deployment': deployment,
                    'classic_ui_url': f"ERROR: {str(e)}"
                })
                csvfile.flush()  # Ensure row is written immediately
                processed_count += 1
        
        csvfile.close()
        
        print(f"\n{'='*60}")
        print(f"✓ Exported {processed_count} results to: {csv_filename}")
        print(f"{'='*60}")
    else:
        print("\nNo account data to process from snowhouse.")
    
finally:
    sf2.close()

