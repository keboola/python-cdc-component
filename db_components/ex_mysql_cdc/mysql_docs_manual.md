
# {prodname} Connector for MySQL

**Category:** debezium-using  
**Type:** assembly

## Overview
MySQL has a binary log (binlog) that records all operations. The {prodname} connector for MySQL uses the binlog to capture all database changes in real-time.

## Configuration Options
- **Connector Class:** `MySql`
- **Connector Name:** MySQL
- **Database Port:** 3306
- **MBean Name:** {context}

### Include List Example
```sql
inventory.*
```

### Features
- **Data Collection:** Table
- **Source Highlighter:** highlight.js
- **MBean Name:** `{mbean-name}`
- **Connector File:** `{connector-file}`

### Documentation Links
- [Getting Started](https://debezium.io/documentation/reference/)
- [Connector Configuration](https://debezium.io/documentation/reference/configuration/)

