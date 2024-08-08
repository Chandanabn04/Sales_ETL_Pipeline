import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.types import Integer, String, Numeric, Date
from sqlalchemy.exc import SQLAlchemyError

file_path='your_file_path'
# Define the PostgreSQL connection details
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432,
    'database': 'database_name'
}

engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# Step 1: Extract the CSV file
try:
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    print("CSV file loaded successfully.")
except Exception as e:
    print(f"Error loading CSV file: {e}")
    
# Step 2: Transformation
print(df.columns)  # To check the column names
df.columns = df.columns.str.strip()  # Strip any leading/trailing whitespace

# Drop unnecessary columns (if they exist)
columns_to_drop = ['id', 'Country', 'Customer_id', 'Product_id', 'user_id']
existing_columns = df.columns.intersection(columns_to_drop)
df.drop(existing_columns, axis=1, inplace=True)

if 'Order_Date' in df.columns:
    df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
    
# Step 3: Load
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

try:
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data loaded into table {table_name} successfully!")
except SQLAlchemyError as e:
    print(f"Error loading data into PostgreSQL: {e}")

