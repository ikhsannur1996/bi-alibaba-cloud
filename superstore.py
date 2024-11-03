from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import csv
import requests

# Define variables for database, table names, and connection IDs
SRC_DB_NAME = "oltp"
SRC_TABLE_NAME = "superstore_oltp"
DEST_DB_NAME = "olap"
DEST_TABLE_NAME = "superstore"
PEOPLES_TABLE_NAME = "peoples"
SRC_CONN_ID = "mysql"
DEST_CONN_ID = "polardb"
CSV_URL = "https://superstorebucket.oss-ap-southeast-5.aliyuncs.com/peoples.csv"
DM_TABLE_NAME="dm_superstore"

def transfer_table(**kwargs):
    # Establish MySQL hooks using connection IDs
    src_hook = MySqlHook(mysql_conn_id=SRC_CONN_ID)
    dest_hook = MySqlHook(mysql_conn_id=DEST_CONN_ID)

    # Step 1: Fetch column names dynamically from the source table
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()
    src_cursor.execute(f"DESCRIBE {SRC_DB_NAME}.{SRC_TABLE_NAME}")
    columns = [column[0] for column in src_cursor.fetchall()]
    
    # Step 2: Fetch all data from the source table
    src_cursor.execute(f"SELECT * FROM {SRC_DB_NAME}.{SRC_TABLE_NAME}")
    data = src_cursor.fetchall()

    # Step 3: Prepare to insert data into the destination table
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    # Clear the target table (optional, based on requirements)
    dest_cursor.execute(f"DELETE FROM {DEST_DB_NAME}.{DEST_TABLE_NAME}")

    # Prepare the insert statement
    insert_stmt = f"INSERT INTO {DEST_DB_NAME}.{DEST_TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

    # Insert all rows at once
    dest_cursor.executemany(insert_stmt, data)
    dest_conn.commit()  # Commit the changes

def insert_csv_data(**kwargs):
    # Download CSV file
    response = requests.get(CSV_URL)
    response.raise_for_status()  # Check if request was successful
    csv_data = response.text.splitlines()
    
    # Parse CSV data with semicolon as the delimiter
    reader = csv.reader(csv_data, delimiter=';')
    headers = next(reader)  # Get column headers from the first row
    
    # Prepare destination connection and insert statement
    dest_hook = MySqlHook(mysql_conn_id=DEST_CONN_ID)
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()
    
    # Clear the target table (optional)
    dest_cursor.execute(f"DELETE FROM {DEST_DB_NAME}.{PEOPLES_TABLE_NAME}")
    
    # Prepare the insert statement
    insert_stmt = f"INSERT INTO {DEST_DB_NAME}.{PEOPLES_TABLE_NAME} ({', '.join(headers)}) VALUES ({', '.join(['%s'] * len(headers))})"
    
    # Insert all rows from CSV at once
    data = [row for row in reader]  # Collect all rows into a list
    dest_cursor.executemany(insert_stmt, data)
    dest_conn.commit()  # Commit changes

def load_dm_table(**kwargs):
    # SQL commands to truncate and reload data into dm_superstore
    truncate_dm_query = f"TRUNCATE TABLE {DEST_DB_NAME}.{DM_TABLE_NAME};"
    insert_dm_query = f"""
    INSERT INTO {DEST_DB_NAME}.{DM_TABLE_NAME}
    SELECT 
        s.category, 
        s.city, 
        s.country, 
        s.customer_id, 
        s.customer_name, 
        s.manufacturer, 
        s.order_date, 
        s.order_id, 
        s.postal_code, 
        s.product_id, 
        s.product_name, 
        s.region, 
        s.row_id, 
        s.segment, 
        s.ship_date, 
        s.ship_mode, 
        s.state, 
        s.sub_category, 
        s.discount, 
        s.profit, 
        s.profit_ratio, 
        s.quantity, 
        s.sales, 
        p.regional_manager 
    FROM 
        {DEST_DB_NAME}.{DEST_TABLE_NAME} s
    LEFT JOIN 
        {DEST_DB_NAME}.{PEOPLES_TABLE_NAME} p 
    ON 
        s.region COLLATE utf8_general_ci = p.region COLLATE utf8_general_ci;
    """
    
    # Execute the SQL commands using MySqlHook
    dest_hook = MySqlHook(mysql_conn_id=DEST_CONN_ID)
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()
    
    # Truncate the existing data in dm_superstore
    dest_cursor.execute(truncate_dm_query)
    
    # Insert new data into dm_superstore
    dest_cursor.execute(insert_dm_query)
    dest_conn.commit()


default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    "superstore",
    default_args=default_args,
    description="DAG to transfer data from a source to a destination MySQL table",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['ikhsan'],
    catchup=False,
) as dag:

    transfer_data = PythonOperator(
        task_id="transfer_table_data",
        python_callable=transfer_table,
        provide_context=True,
    )

    load_csv_data = PythonOperator(
        task_id="load_csv_data",
        python_callable=insert_csv_data,
        provide_context=True,
    )

    load_dm = PythonOperator(
    task_id="load_dm_table",
    python_callable=load_dm_table,
    provide_context=True,
    )

    # Define task dependencies
    transfer_data >> load_csv_data >> load_dm
