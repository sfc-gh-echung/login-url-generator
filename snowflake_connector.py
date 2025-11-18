"""
Snowflake Python Connector with External Browser Authentication
"""
import snowflake.connector
from snowflake.connector import DictCursor
import os
from typing import Optional


class SnowflakeConnection:
    """
    A class to manage Snowflake connections using external browser authentication.
    """
    
    def __init__(
        self,
        account: str,
        user: str,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None
    ):
        """
        Initialize Snowflake connection parameters.
        
        Args:
            account: Snowflake account identifier
            user: Snowflake username
            warehouse: Default warehouse to use (optional)
            database: Default database to use (optional)
            schema: Default schema to use (optional)
            role: Default role to use (optional)
        """
        self.account = account
        self.user = user
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """
        Establish connection to Snowflake using external browser authentication.
        This will open a browser window for SSO/SAML authentication.
        """
        try:
            # Parse account identifier for custom host/port
            connection_params = {
                'user': self.user,
                'authenticator': 'externalbrowser',
            }
            
            # Handle custom host:port format
            if ':' in self.account:
                # Split host and port
                host, port = self.account.rsplit(':', 1)
                connection_params['host'] = host
                connection_params['port'] = int(port)
                # Extract account name from host (everything before .snowflakecomputing.com)
                if '.snowflakecomputing.com' in host:
                    account_name = host.split('.snowflakecomputing.com')[0]
                    connection_params['account'] = account_name
                else:
                    connection_params['account'] = host
            else:
                connection_params['account'] = self.account
            
            # Add optional parameters if provided
            if self.warehouse:
                connection_params['warehouse'] = self.warehouse
            if self.database:
                connection_params['database'] = self.database
            if self.schema:
                connection_params['schema'] = self.schema
            if self.role:
                connection_params['role'] = self.role
            
            print(f"Connecting to Snowflake account: {self.account}")
            print("A browser window will open for authentication...")
            
            self.connection = snowflake.connector.connect(**connection_params)
            self.cursor = self.connection.cursor(DictCursor)
            
            print("✓ Successfully connected to Snowflake!")
            return self.connection
            
        except Exception as e:
            print(f"✗ Error connecting to Snowflake: {str(e)}")
            raise
    
    def execute_query(self, query: str, fetch: bool = True):
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string to execute
            fetch: Whether to fetch and return results (default: True)
            
        Returns:
            Query results if fetch=True, otherwise None
        """
        if not self.cursor:
            raise Exception("Not connected to Snowflake. Call connect() first.")
        
        try:
            self.cursor.execute(query)
            
            if fetch:
                results = self.cursor.fetchall()
                return results
            else:
                return None
                
        except Exception as e:
            print(f"✗ Error executing query: {str(e)}")
            raise
    
    def close(self):
        """
        Close the Snowflake connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed.")


def main():
    """
    Example usage of the SnowflakeConnection class.
    """
    # Configuration
    ACCOUNT = "snowflake.prod1.us-west-2.external-zone.snowflakecomputing.com:8085"
    USER = os.getenv('SNOWFLAKE_USER', 'your_username')  # Set via environment variable or replace
    
    # Optional: Specify defaults
    WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', None)
    DATABASE = os.getenv('SNOWFLAKE_DATABASE', None)
    SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', None)
    ROLE = os.getenv('SNOWFLAKE_ROLE', None)
    
    # Create connection instance
    sf = SnowflakeConnection(
        account=ACCOUNT,
        user=USER,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role=ROLE
    )
    
    try:
        # Connect using external browser
        sf.connect()
        
        # Example: Get current account and user info
        print("\n--- Testing Connection ---")
        result = sf.execute_query("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE()")
        for row in result:
            print(f"Account: {row['CURRENT_ACCOUNT()']}")
            print(f"User: {row['CURRENT_USER()']}")
            print(f"Role: {row['CURRENT_ROLE()']}")
        
        # Example: List databases
        print("\n--- Available Databases ---")
        databases = sf.execute_query("SHOW DATABASES")
        for db in databases[:5]:  # Show first 5
            print(f"  - {db['name']}")
        
        # Example: List warehouses
        print("\n--- Available Warehouses ---")
        warehouses = sf.execute_query("SHOW WAREHOUSES")
        for wh in warehouses[:5]:  # Show first 5
            print(f"  - {wh['name']}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Always close the connection
        sf.close()


if __name__ == "__main__":
    main()

