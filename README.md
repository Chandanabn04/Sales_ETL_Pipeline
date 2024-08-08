# ETL Project: CSV to PostgreSQL

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Python, Pandas, and SQLAlchemy. The pipeline extracts data from a CSV file, performs basic transformations, and loads the data into a PostgreSQL database.

## Prerequisites

Before running the code, ensure you have the following installed:

- Python 3.x
- PostgreSQL
- `pandas` library
- `SQLAlchemy` library
- `psycopg2` library

You can install the required Python libraries using pip:

```bash
pip install pandas sqlalchemy psycopg2
```

## Project Structure

- **superstore.csv**: The input CSV file containing sales data.
- **etl_script.py**: The Python script to perform the ETL process.

## Steps Performed in the Script

### 1. Define PostgreSQL Connection Details

The script starts by defining the PostgreSQL connection details, such as username, password, host, port, and database name. These details are stored in a dictionary and used to create a connection string for SQLAlchemy.

```python
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432,
    'database': 'database_name'
}

engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
```

### 2. Extract Data from CSV File

The script reads the data from the `superstore.csv` file using Pandas, with the appropriate encoding (`ISO-8859-1`).

```python
df = pd.read_csv('superstore.csv', encoding='ISO-8859-1')
```

### 3. Transform the Data

The script performs the following transformations:

- Prints the column names for inspection.
- Strips any leading or trailing whitespace from the column names.
- Drops unnecessary columns if they exist in the DataFrame.

```python
print(df.columns)  # To check the column names
df.columns = df.columns.str.strip()  # Strip any leading/trailing whitespace

# Drop unnecessary columns
columns_to_drop = ['id', 'Country', 'Customer_id', 'Product_id', 'user_id']
existing_columns = df.columns.intersection(columns_to_drop)
df.drop(existing_columns, axis=1, inplace=True)
```

### 4. Load the Data into PostgreSQL

The script infers the table schema automatically based on the CSV data types and creates a table in PostgreSQL. It then loads the data into the table.

```python
# Automatically infer the table schema based on the CSV file
metadata = MetaData()

# Map pandas dtypes to SQLAlchemy types
dtype_mapping = {
    'int64': Integer,
    'float64': Numeric,
    'object': String,
    'datetime64[ns]': Date,
}

# Dynamically create columns based on CSV headers and inferred data types
columns = []
for column_name, dtype in df.dtypes.items():
    sqlalchemy_type = dtype_mapping.get(str(dtype), String)
    columns.append(Column(column_name, sqlalchemy_type))

# Create a table dynamically
table_name = 'sales_data'
table = Table(table_name, metadata, *columns)

metadata.create_all(engine)

df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Data loaded into table {table_name} successfully!")
```

## How to Run the Script

1. Clone the repository or download the script to your local machine.
2. Ensure that your PostgreSQL server is running, and you have created the required database.
3. Update the `db_config` dictionary with your PostgreSQL credentials.
4. Place the `superstore.csv` file in the same directory as the script.
5. Run the script:

```bash
python etl_script.py
```

If successful, the data from the CSV file will be loaded into the PostgreSQL database in a table named `sales_data`.
