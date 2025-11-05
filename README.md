# 📊 Call Center Analytics - Data Infrastructure (No Analysis)

## Overview

This project provides **data infrastructure only** - all the methods and utilities that analysts need to perform their own analysis. No pre-built analysis or aggregations included.

## 🎯 What This Provides

### ✅ Data Infrastructure
- PostgreSQL schema for detailed storage
- ClickHouse schema for fast querying
- Data loading from JSONL files
- Data synchronization between databases

### ✅ Utility Methods for Analysts
- Query helpers for common patterns
- Data export functions (CSV, JSON)
- Conversation retrieval
- Feature access methods
- Data validation utilities

### ❌ What's NOT Included
- No pre-computed statistics
- No dashboard aggregations
- No analysis logic
- No visualization code

**Analysts decide what analysis to perform!**

## 🚀 Quick Reference

### For Data Engineers (Loading Data)

```bash
# Initialize databases
python main.py init

# Load conversation data
python main.py ingest

# Sync to ClickHouse
python main.py sync

# Validate data
python main.py validate

# Full pipeline
python main.py run-full
```

### For Analysts (Using Data)

```python
from src.analyst_helper import AnalystHelper

helper = AnalystHelper()

# Query data
calls = helper.get_calls_by_date_range('2025-10-01', '2025-10-31')
bad_word_calls = helper.get_calls_with_bad_words()
conversation = helper.get_conversation('call-uuid')

# Export data
helper.export_date_range('2025-10-01', '2025-10-31', 'october.csv')

# Run custom queries
results = helper.run_custom_query("SELECT ... FROM ...")

helper.close()
```

## 📁 Project Structure

```
cca-db/
├── main.py                          # Data pipeline (init, ingest, sync, validate)
├── src/
│   ├── dbs.py                       # Database connection classes
│   ├── analyst_helper.py            # 🔑 Helper for analysts
│   └── loaders/
│       ├── postgres_loader.py       # Load data to PostgreSQL
│       └── clickhouse_loader.py     # Sync to ClickHouse + query utilities
├── db/
│   ├── migrations/001_init.sql      # PostgreSQL schema
│   └── clickhouse/001_init.sql      # ClickHouse schema
└── data/
    ├── conversations/               # Input: conversation JSONL
    └── conversations_output/        # Input: feature extraction output
```

## 📚 Documentation

- **QUICKSTART.md** - Get started in 5 minutes
- **ANALYST_GUIDE.md** - 🔑 Complete guide for analysts
- **README_DETAILED.md** - Technical documentation
- **IMPLEMENTATION_SUMMARY.md** - What was built

## 🔧 Available Utility Methods

### Query Helpers (in AnalystHelper)

```python
# Date-based queries
get_calls_by_date_range(start_date, end_date)
get_calls_by_operator(operator_id, limit)
get_calls_by_category(category, subcategory)

# Filter-based queries
get_calls_with_bad_words()
get_calls_without_greeting()
get_negative_sentiment_calls(role)

# Get specific calls with multiple filters
get_calls_with_filters(
    sentiment_expert='NEGATIVE',
    sentiment_subscriber='POSITIVE',
    has_bad_words=True,
    has_greeting=False,
    min_duration=60,
    max_duration=300
)

# Conversation details
get_conversation(call_id)        # Get messages
get_call_details(call_id)        # Get messages + features
```

### Export Methods

```python
# Export to CSV
export_date_range(start, end, 'file.csv')
export_operator_calls(operator_id, 'file.csv')
export_category_calls(category, 'file.csv')
export_filtered_calls(filters_dict, 'file.csv')

# Export to JSON
export_to_json(query, 'file.json', columns)
```

### Data Utilities

```python
# Summary and validation
get_data_summary()              # Total calls, date range
get_sample_data(limit)          # Sample records
validate_data_quality()         # Check sync status
get_table_count(table)          # Row count
get_date_range(table, column)   # Date range

# Custom queries
run_custom_query(sql, database)  # Run any SQL query
execute_custom_query(query, 'clickhouse' or 'postgres')
```

## 📊 Available Data

### ClickHouse (Fast Analytics)
- `call_center_analytics.call_analytics` - All call data with metrics

**Fields:** call_id, dates, durations, message counts, sentiments, quality flags, summaries, categories

### PostgreSQL (Detailed Data)
- `calls` - Call metadata
- `call_messages` - Individual messages (role, order, text)
- `call_features` - Extracted features + full JSON
- `users` - Customer info
- `operators` - Operator info

## 🔍 Example Analysis Workflows

### 1. Quality Audit
```python
helper = AnalystHelper()

# Find calls without proper greeting
no_greeting = helper.get_calls_without_greeting()

# Export for review
helper.export_filtered_calls(
    {'has_greeting': False},
    'missing_greetings.csv'
)

helper.close()
```

### 2. Sentiment Analysis
```python
helper = AnalystHelper()

# Get negative subscriber calls
negative = helper.get_negative_sentiment_calls('subscriber')

# Review conversations
for call in negative[:10]:
    details = helper.get_call_details(call[0])
    # Your analysis here

helper.close()
```

### 3. Category Trends
```python
helper = AnalystHelper()

query = """
SELECT 
    category,
    toDate(load_date) as date,
    count() as calls
FROM call_center_analytics.call_analytics
WHERE category != ''
GROUP BY category, date
ORDER BY date, category
"""

results = helper.run_custom_query(query)
# Export or analyze with pandas
helper.close()
```

### 4. Operator Performance
```python
helper = AnalystHelper()

# Get all calls for operator
calls = helper.get_calls_by_operator('operator-uuid')

# Export for detailed review
helper.export_operator_calls('operator-uuid', 'operator_calls.csv')

# Custom analysis query
query = """
SELECT 
    count() as total,
    avg(duration_seconds) as avg_duration,
    avgIf(start_greeting_expert, start_greeting_expert=1) as greeting_rate
FROM call_center_analytics.call_analytics
WHERE operator_id = 'operator-uuid'
"""
stats = helper.run_custom_query(query)

helper.close()
```

## 🔌 Integration Examples

### With Pandas
```python
import pandas as pd
from src.analyst_helper import AnalystHelper

helper = AnalystHelper()
results = helper.get_calls_by_date_range('2025-10-01', '2025-10-31')
df = pd.DataFrame(results, columns=[...])
# Your pandas analysis
helper.close()
```

### With Jupyter
```python
# In notebook
from src.analyst_helper import AnalystHelper
helper = AnalystHelper()
# Interactive analysis
helper.close()
```

### Direct SQL
```python
from src.dbs import ClickHouse

ch = ClickHouse()
results = ch.execute_query("YOUR SQL HERE")
ch.disconnect()
```

## ✨ Key Features

1. **Flexible Querying** - Pre-built methods + custom SQL support
2. **Multiple Export Formats** - CSV, JSON for external tools
3. **Easy Data Access** - Simple helper class for common tasks
4. **Direct Database Access** - Full control when needed
5. **Data Validation** - Built-in quality checks
6. **Fast Analytics** - ClickHouse for aggregations
7. **Detailed Access** - PostgreSQL for conversation text
8. **No Lock-in** - Use any analysis tool you want

## 🎓 Learn More

- **For Analysts**: Read `ANALYST_GUIDE.md`
- **For Data Engineers**: Read `README_DETAILED.md`
- **Quick Start**: See `QUICKSTART.md`
- **Code Examples**: Check `src/analyst_helper.py`

## 💡 Philosophy

**Data infrastructure provides the foundation.**
**Analysts provide the intelligence.**

We give you:
- Clean, organized data
- Fast query capabilities
- Useful utility methods
- Export functionality

You bring:
- Domain expertise
- Analysis requirements
- Statistical methods
- Business insights

**Analysis is YOUR job. We just make it easier!**
