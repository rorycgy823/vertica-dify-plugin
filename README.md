# Vertica Database Plugin for Dify

A Dify plugin that provides comprehensive Vertica database operations through natural language. This plugin enables AI assistants to query, explore, and analyze Vertica databases safely and efficiently.

## Features

- **🔒 Safety-First Design**: Readonly mode by default with configurable write permissions
- **⚡ High Performance**: Connection pooling, streaming queries, and optimized batch processing
- **🔍 Schema Discovery**: Complete database exploration tools
- **📊 Analytics Ready**: Supports complex queries and large dataset handling
- **🔧 Production Ready**: SSL support, timeout configuration, and robust error handling

## Available Tools

### Query Execution
- **Execute SQL Query**: Run SQL queries with parameterized support
- **Stream Query Results**: Handle large datasets efficiently with configurable batching

### Schema Discovery
- **List Tables**: Get all tables in a schema with metadata
- **List Views**: Retrieve views with their definitions
- **List Indexes**: Get projection information (Vertica's equivalent of indexes)
- **Get Table Structure**: Detailed column information, types, and constraints

## Installation

### From Dify Marketplace
1. Open Dify and go to **Plugins** > **Plugin Marketplace**
2. Search for "Vertica Database"
3. Click **Install**

### Manual Installation
1. Download the plugin package (`vertica.difypkg`)
2. In Dify, go to **Plugins** > **Install Custom Plugin**
3. Upload the package file

## Configuration

### Required Settings
- **Vertica Host**: Database server hostname or IP address
- **Database Name**: Name of the Vertica database
- **Username**: Database user with appropriate permissions

### Optional Settings
- **Port**: Database port (default: 5433)
- **Password**: User password (if required)
- **Readonly Mode**: Enable safety restrictions (default: enabled)
- **Connection Limit**: Maximum concurrent connections (default: 10)
- **Query Timeout**: Execution timeout in milliseconds (default: 60000)
- **SSL Enabled**: Use SSL/TLS encryption (default: disabled)
- **Default Schema**: Default schema for queries (default: public)

## Usage Examples

### Basic Query
```sql
SELECT customer_state, COUNT(*) as count
FROM customer_dimension
GROUP BY customer_state
ORDER BY count DESC
LIMIT 10;
```

### Schema Exploration
```sql
-- List all tables in the public schema
-- Use the "List Tables" tool

-- Get detailed structure of a table
-- Use the "Get Table Structure" tool with table_name="customer_dimension"
```

### Large Dataset Handling
```sql
-- For queries returning >10,000 rows, use "Stream Query Results"
-- Set batch_size=1000 and max_rows as needed
SELECT * FROM sales_fact WHERE sale_date >= '2024-01-01';
```

## Security & Safety

### Readonly Mode (Recommended)
When enabled, only these query types are allowed:
- `SELECT` statements
- `SHOW` commands
- `DESCRIBE` statements
- `EXPLAIN` plans
- `WITH` clauses

### Write Operations
To enable INSERT/UPDATE/DELETE/CREATE/DROP operations:
1. Set **Readonly Mode** to `false` in plugin configuration
2. Ensure database user has appropriate permissions
3. **Warning**: Only disable readonly mode if you understand the security implications

## Performance Optimization

### Connection Management
- Automatic connection pooling
- Configurable connection limits
- Connection reuse for multiple queries

### Query Optimization
- Parameterized queries prevent SQL injection
- Streaming for large result sets
- Configurable timeouts prevent hanging queries

### Vertica-Specific Features
- Projection-aware queries (Vertica's indexing system)
- Optimized for columnar storage
- Support for complex analytical queries

## Troubleshooting

### Connection Issues
- Verify host, port, and database name
- Check user credentials and permissions
- Ensure network connectivity to Vertica server

### Query Timeouts
- Increase **Query Timeout** setting for complex queries
- Consider using streaming for large datasets
- Optimize query performance on the database side

### Permission Errors
- Ensure user has SELECT permissions on queried tables
- For schema discovery, user needs access to system catalogs
- Check readonly mode settings

## Requirements

- **Dify**: Community Edition v1.0.0 or later
- **Vertica Database**: Any recent version
- **Network Access**: Ability to connect to Vertica server
- **Python Dependencies**: Automatically managed by Dify

## Support

- **Documentation**: [Dify Plugin Documentation](https://docs.dify.ai/plugins)
- **Issues**: Report bugs via Dify's plugin system
- **Community**: Join Dify community discussions

## License

This plugin is released under the MIT License. See the original [Vertica MCP Server](https://github.com/hechtcarmel/vertica-mcp) for more information.

## Acknowledgments

This plugin is based on the [Vertica MCP Server](https://github.com/hechtcarmel/vertica-mcp) by [@hechtcarmel](https://github.com/hechtcarmel), adapted for the Dify plugin ecosystem.
