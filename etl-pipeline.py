import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.types import Integer, String, Numeric, Date
from sqlalchemy.exc import SQLAlchemyError

def load_csv(file_path):
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')
        print("CSV file loaded successfully.")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

def transform_data(df):
    if df is None:
        return None
    print(df.columns)  # To check the column names
    df.columns = df.columns.str.strip()  # Strip any leading/trailing whitespace

    # Drop unnecessary columns (if they exist)
    columns_to_drop = ['id', 'Country', 'Customer_id', 'Product_id', 'user_id', 'order_s', 'state_id']
    existing_columns = df.columns.intersection(columns_to_drop)
    df.drop(existing_columns, axis=1, inplace=True)

    if 'Order_Date' in df.columns:
        df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
    
    return df

def load_data_to_db(df, db_config, table_name):
    if df is None:
        return
    engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

    metadata = MetaData()
    dtype_mapping = {
        'int64': Integer,
        'float64': Numeric,
        'object': String,
        'datetime64[ns]': Date,
    }

    columns = [Column(name, dtype_mapping.get(str(dtype), String)) for name, dtype in df.dtypes.items()]
    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)

    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data loaded into table {table_name} successfully!")
    except SQLAlchemyError as e:
        print(f"Error loading data into PostgreSQL: {e}")

def etl_process(file_path, db_config, table_name):
    df = load_csv(file_path)
    df = transform_data(df)
    load_data_to_db(df, db_config, table_name)

# Define the PostgreSQL connection details
db_config = {
    'user': 'YourUser',
    'password': 'YourPassword', 
    'host': 'localhost',
    'port': 5432,
    'database': 'YourDB'
}

file_path = 'superstore.csv'
table_name = 'YourTableName'
etl_process(file_path, db_config, table_name)
