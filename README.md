# Snowflake Python Connector with External Browser Authentication

This project provides a Python connector for Snowflake that uses external browser authentication (SSO/SAML).

## Features

- External browser authentication (SSO/SAML)
- Easy-to-use connection class
- Support for query execution with results
- Proper connection management and cleanup

## Prerequisites

- Python 3.7 or higher
- A Snowflake account with SSO/SAML authentication enabled
- Your Snowflake username

## Installation

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

You can configure the connection using environment variables or by modifying the script directly.

### Environment Variables (Recommended)

```bash
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_WAREHOUSE="your_warehouse"  # Optional
export SNOWFLAKE_DATABASE="your_database"    # Optional
export SNOWFLAKE_SCHEMA="your_schema"        # Optional
export SNOWFLAKE_ROLE="your_role"            # Optional
```

### Direct Configuration

Edit `snowflake_connector.py` and update the following variables in the `main()` function:

```python
ACCOUNT = "snowflake.prod1.us-west-2.external-zone.snowflakecomputing.com:8085"
USER = "your_username"
WAREHOUSE = "your_warehouse"  # Optional
DATABASE = "your_database"    # Optional
SCHEMA = "your_schema"        # Optional
ROLE = "your_role"            # Optional
```

## Usage

### Basic Usage

Run the example script:

```bash
python snowflake_connector.py
```

This will:
1. Open your default browser for authentication
2. Connect to Snowflake after successful authentication
3. Run some example queries to verify the connection

### Using in Your Own Code

```python
from snowflake_connector import SnowflakeConnection

# Create connection
sf = SnowflakeConnection(
    account="snowflake.prod1.us-west-2.external-zone.snowflakecomputing.com:8085",
    user="your_username",
    warehouse="your_warehouse",  # Optional
    database="your_database",     # Optional
    schema="your_schema"          # Optional
)

try:
    # Connect using external browser
    sf.connect()
    
    # Execute queries
    results = sf.execute_query("SELECT * FROM your_table LIMIT 10")
    
    for row in results:
        print(row)
    
finally:
    # Always close the connection
    sf.close()
```

## How External Browser Authentication Works

1. When you run the script, it will print a message indicating that a browser window will open
2. Your default web browser will automatically open with your organization's SSO/SAML login page
3. After successful authentication in the browser, the script will establish the connection
4. The authentication token is cached, so subsequent connections may not require re-authentication

## Troubleshooting

### Browser doesn't open automatically

- Make sure you have a default browser set in your system
- Check if any firewall or security software is blocking the browser launch

### Authentication fails

- Verify your username is correct
- Ensure you have access to the Snowflake account
- Check that external browser authentication is enabled for your account

### Connection timeout

- Verify the account URL is correct
- Check your network connection
- Ensure you're not behind a proxy that might block the connection

### Port 8085 issues

If you encounter issues with port 8085, you may need to adjust the account identifier. Try one of these formats:
- `snowflake.prod1.us-west-2.external-zone.snowflakecomputing.com:8085`
- `snowflake.prod1.us-west-2.external-zone` (without port)

## Additional Resources

- [Snowflake Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [External Browser Authentication](https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-sso-with-external-browser)

## License

MIT License - Feel free to use and modify as needed.

## To Run

```
python generate_urls.py --query-file queries/query_scim_users_never_logged_in.sql --skip-processed-check

python upload_urls_to_snowflake.py --csv snowflake_urls_20251120_232939.csv --table login_urls_20251120_run
```