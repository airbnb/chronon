# Sample Data for Chronon Bootcamp

This directory contains pre-generated Parquet files for the Chronon GroupBy bootcamp.

## Files

- **`purchases.parquet`** - Purchase event data (~155 records)
- **`users.parquet`** - User demographic data (100 records)

## Data Overview

### Purchases Table
- **Records**: ~155 purchase events
- **Date Range**: December 1-7, 2023
- **Columns**:
  - `user_id`: User identifier (user_1 to user_100)
  - `purchase_price`: Purchase amount ($2.58 - $89.69)
  - `item_category`: Product category (electronics, books, clothing, food, home)
  - `ts`: Purchase timestamp

### Users Table
- **Records**: 100 users
- **Columns**:
  - `user_id`: User identifier (user_1 to user_100)
  - `age`: User age (18-65)
  - `city`: User city (New York, San Francisco, Chicago, Miami)
  - `signup_date`: User signup date

## Usage

The setup script automatically loads these files into Spark tables:
- `data.purchases` - Purchase events
- `data.users` - User demographics

## Regenerating Data

To regenerate the sample data with different parameters:

```bash
python3 scripts/generate_sample_parquet.py
```

This will create new Parquet files with the same structure but different random data.
