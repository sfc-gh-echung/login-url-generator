"""
Simple example of connecting to Snowflake with external browser authentication.
"""
from snowflake_connector import SnowflakeConnection

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
    
    # Query to get account information
    print("\n--- Querying Account Information from Snowhouse ---")
    query = """
    SELECT 
        a.name as account_name,
        a.deployment,
        drc.snowflake_region,
        drc.snowflake_region_group || '.' || drc.snowflake_region as classic_ui_url
    FROM snowhouse_import.prod.account_etl_v a
    LEFT JOIN snowhouse_import.public.deployment_region_cloud drc 
        ON a.deployment = drc.deployment
    WHERE a.name = 'NFB51686'
    """
    results = sf1.execute_query(query)
    
    if results:
        print(f"\nFound {len(results)} result(s):")
        for row in results:
            print(f"  Account Name: {row['ACCOUNT_NAME']}")
            print(f"  Deployment: {row['DEPLOYMENT']}")
            print(f"  Snowflake Region: {row['SNOWFLAKE_REGION']}")
            print(f"  Classic UI URL Parameter: {row['CLASSIC_UI_URL']}")
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
        print("\n--- Running SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL ---")
        for row in results:
            account_name = row['ACCOUNT_NAME']
            classic_ui_url = row['CLASSIC_UI_URL']
            
            print(f"\nGenerating URL for account: {account_name}")
            print(f"Using region parameter: {classic_ui_url}")
            
            query = f"SELECT SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL('{account_name}', '{classic_ui_url}')"
            try:
                result = sf2.execute_query(query)
                url = result[0][f"SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL('{account_name}', '{classic_ui_url}')"]
                print(f"✓ Generated URL: {url}")
            except Exception as e:
                print(f"✗ Error: {str(e)}")
    else:
        print("\nNo account data to process from snowhouse.")
    
finally:
    sf2.close()

