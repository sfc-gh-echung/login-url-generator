# Snowflake Login URL Generator

A Python tool for generating Snowflake Classic UI login URLs for multiple accounts and uploading them to Snowflake for downstream processing (e.g., email campaigns).

## Overview

This tool connects to Snowhouse to query account information, generates direct login URLs using Snowflake's `SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL` function, and optionally uploads the results to a Snowflake table for use in marketing campaigns or user activation flows.

## Features

- **Multi-account URL generation**: Generate login URLs for multiple Snowflake accounts in batch
- **Flexible querying**: Support for custom SQL queries to filter accounts
- **Duplicate prevention**: Skip already processed accounts to avoid redundant work
- **CSV export**: Export results to timestamped CSV files
- **Snowflake upload**: Upload generated URLs to Snowflake tables with error filtering
- **External browser authentication**: SSO/SAML authentication via browser
- **Batch processing**: Efficient batch inserts for large datasets

## Prerequisites

- Python 3.7 or higher
- A Snowflake account with access to:
  - Snowhouse (for account metadata)
  - External zone (for URL generation function)
- SSO/SAML authentication enabled
- Access to the `SYSTEM$GET_GLOBAL_ACCOUNT_CLASSIC_UI_URL` function

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd login-url-generator
```

2. Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

### User Settings

Update the following values in the scripts to match your environment:

**`generate_urls.py`:**
- `USER1` and `USER2`: Your Snowflake username (lines 41, 111)
- `EXISTING_CSV`: Path to existing CSV for duplicate checking (line 19)

**`upload_urls_to_snowflake.py`:**
- `USER`: Your Snowflake username (line 12)
- `DATABASE`: Target database (line 13)
- `SCHEMA`: Target schema (line 14)

## Usage

### 1. Generate Login URLs

Generate URLs for all active accounts:

```bash
python generate_urls.py
```

Generate URLs using a custom query:

```bash
python generate_urls.py --query-file queries/query_scim_users_never_logged_in.sql
```

Skip duplicate checking and process all accounts:

```bash
python generate_urls.py --skip-processed-check
```

Combine options:

```bash
python generate_urls.py \
  --query-file queries/query_scim_users_never_logged_in.sql \
  --skip-processed-check
```

**Output:** Creates a timestamped CSV file (e.g., `snowflake_urls_20251121_090303.csv`) with columns:
- `account_name`: Snowflake account name
- `deployment`: Account deployment ID
- `classic_ui_url`: Generated login URL (or error message if generation failed)

### 2. Upload URLs to Snowflake

Upload the generated CSV to a Snowflake table:

```bash
python upload_urls_to_snowflake.py \
  --csv snowflake_urls_20251120_232939.csv \
  --table login_urls_20251120_run
```

**What this does:**
- Reads the CSV file
- Filters out rows with errors or empty values
- Creates or replaces the specified table
- Inserts valid rows in batches of 1000
- Reports success/failure statistics

**Target table:** `temp.echung2.<table_name>` (configurable in script)

### 3. Using the Snowflake Connector Directly

The `snowflake_connector.py` module can be used independently:

```python
from snowflake_connector import SnowflakeConnection

# Create connection
sf = SnowflakeConnection(
    account="your-account",
    user="your_username",
    database="your_database",  # Optional
    schema="your_schema"       # Optional
)

try:
    # Connect (opens browser for SSO)
    sf.connect()
    
    # Execute query
    results = sf.execute_query("SELECT * FROM my_table LIMIT 10")
    
    for row in results:
        print(row)
finally:
    sf.close()
```

## SQL Queries

The `queries/` directory contains pre-built queries:

### `all_active_accounts.sql`
Returns all active (non-suspended, non-deleted) Snowflake accounts.

### `query_scim_users_never_logged_in.sql`
Returns accounts with SCIM-provisioned users who have never logged in, filtered by creation date. Useful for activation email campaigns.

**Key filters:**
- Active accounts only
- SCIM users only
- Never logged in (`LAST_SUC_LOGIN IS NULL`)
- Created within specific date range

You can create custom queries following the same pattern. Required columns:
- `account_name`: Account identifier
- `deployment`: Deployment ID
- `classic_ui_url`: Region parameter for URL generation

## HTML Email Templates

The `html_emails/` directory contains email templates that use the generated URLs:

- `oauth_activation_welcome_email.html`: Welcome email for new OAuth users
- `ai_sql_reengagement_email.html`: Re-engagement email for AI/SQL features

## Workflow Example

**Use case:** Send activation emails to SCIM users who haven't logged in yet

1. **Generate URLs for target users:**
   ```bash
   python generate_urls.py \
     --query-file queries/query_scim_users_never_logged_in.sql
   ```

2. **Upload to Snowflake:**
   ```bash
   python upload_urls_to_snowflake.py \
     --csv snowflake_urls_20251121_090303.csv \
     --table login_urls_activation_campaign
   ```

3. **Use in email campaign:**
   - Query the table `temp.echung2.login_urls_activation_campaign`
   - Join with user data to get email addresses
   - Use the `classic_ui_url` in your email template
   - Send via your email marketing platform

## File Structure

```
login-url-generator/
├── generate_urls.py           # Main URL generation script
├── upload_urls_to_snowflake.py # Upload CSV to Snowflake
├── snowflake_connector.py     # Snowflake connection utility
├── requirements.txt           # Python dependencies
├── queries/                   # SQL query templates
│   ├── all_active_accounts.sql
│   └── query_scim_users_never_logged_in.sql
├── html_emails/              # Email templates
│   ├── oauth_activation_welcome_email.html
│   └── ai_sql_reengagement_email.html
└── login_urls/               # Output directory for CSV files
```

## Troubleshooting

### Browser doesn't open for authentication
- Ensure you have a default browser configured
- Check firewall settings that might block browser launches
- Try authenticating manually at https://app.snowflake.com

### "Already processed" skipping too many accounts
- Check the path to `EXISTING_CSV` in `generate_urls.py`
- Use `--skip-processed-check` flag to disable duplicate checking
- Delete or rename old CSV files if starting fresh

### URL generation errors
Common errors in the output CSV:
- `Account not found`: Account doesn't exist or you don't have access
- `Invalid region`: Region parameter is malformed
- `Timeout`: Network or authentication issues

### Upload fails with permissions error
- Verify you have INSERT privileges on the target database/schema
- Check that the database and schema exist
- Ensure you're using the correct role with proper permissions

### Port 8085 connection issues
The external zone uses a custom port. If you have connectivity issues:
- Ensure port 8085 is not blocked by your firewall
- Try connecting from a different network
- Contact your Snowflake administrator

## Advanced Usage

### Custom Account Queries

Create your own SQL query file following this structure:

```sql
SELECT 
    a.name as account_name,
    a.deployment,
    drc.snowflake_region_group || '.' || drc.snowflake_region as classic_ui_url
FROM SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a
LEFT JOIN snowhouse_import.public.deployment_region_cloud drc 
    ON a.deployment = drc.deployment
WHERE 
    -- Add your custom filters here
    a.SUSPENDED_TIME IS NULL
    AND a.DELETED_ON IS NULL
```

### Batch Configuration

Adjust the batch size for uploads in `upload_urls_to_snowflake.py`:

```python
BATCH_SIZE = 1000  # Increase for better performance, decrease if encountering memory issues
```

### Multiple CSV Combination

To combine multiple CSV files:

```bash
# Combine all CSVs in current directory
cat snowflake_urls_*.csv | grep -v "^account_name,deployment" | \
  cat <(head -1 snowflake_urls_20251121_090303.csv) - > combined_urls.csv
```

## Security Notes

- Credentials are never stored; authentication uses SSO/SAML via browser
- Generated URLs are direct login links - handle with care
- Consider implementing URL expiration in downstream systems
- Restrict access to Snowflake tables containing login URLs

## License

MIT License - Feel free to use and modify as needed.

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review [Snowflake Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
3. Review [External Browser Authentication](https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-sso-with-external-browser)
