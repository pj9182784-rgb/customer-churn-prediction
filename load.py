import mysql.connector as sc
import pandas as pd
import os
import shutil

def db_connection():
    conn = sc.connect(
        host='localhost',
        user='root',
        password='Root@123',
        database='pradip',
        port=3306
    )
    cursor = conn.cursor()
    return cursor, conn

def load_files(table_name, file_path):
    if os.path.exists(file_path):
        cursor, conn = db_connection()
        
        # Delete previous records from this file
        sql = f"DELETE FROM {table_name} WHERE file_name = %s"
        cursor.execute(sql, (file_path,))
        conn.commit()

        # Load file based on extension
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith(".txt"):
            df = pd.read_csv(file_path, delimiter="|")  # adjust delimiter if needed
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path)
        elif file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format")

        # Add extra columns
        df['file_name'] = file_path
        df['user_name'] = 'pradip'
        df['rownumber'] = range(1, len(df) + 1)

        # Build SQL insert query
        column_names = df.columns.tolist()
        columns_str = ", ".join(f"`{col}`" for col in column_names)
        placeholders = ", ".join(["%s"] * len(column_names))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Execute insert
        records = [tuple(row) for row in df.values.tolist()]
        cursor.executemany(insert_sql, records)

        update_query = f"update {table_name} set Last_updated_at  = CURRENT_TIMESTAMP() WHERE file_name = '{file_path}'  ;"
        cursor.execute(update_query)
        conn.commit()
        cursor.close()
        conn.close()
        print(f'Data has been loaded successfully into {table_name}')
        
        source =file_path
        destination = "data_folder\processed_data"

        # Step 1: Copy the file
        shutil.copy(source, destination)
        print("File copied!")

        # Step 2: Remove the original file
        os.remove(source)
        print("Source file removed!")
    else:
        print(f'we dont able to find the {file_path}')

# Data to load
data = [
    {'table_name':'ecommerce_order_raw_table', 'file_path':'data_folder\\unprocessed_data\\ecommerce_orders.csv'},
    {'table_name':'crm_customer_raw_table','file_path':'data_folder\\unprocessed_data\\crm_customers.xlsx'},
    {'table_name':'marketing_events_raw_table','file_path':'data_folder\\unprocessed_data\\marketing_events.json'},
    {'table_name':'support_tickets_raw_table','file_path':'data_folder\\unprocessed_data\\support_tickets.txt'}
]

# Load all files
for details in data:
    load_files(details['table_name'], details['file_path'])


