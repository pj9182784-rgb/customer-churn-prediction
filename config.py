import mysql.connector as sc
import pandas as pd
import os
import shutil
import glob

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

def load_file(table_name, file_path):
    if os.path.exists(file_path):
        cursor, conn = db_connection()
        
        # Delete previous records from this file
        sql = f"DELETE FROM {table_name} WHERE file_name = %s"
        cursor.execute(sql, (file_path,))
        conn.commit()

        # Load the file
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith(".txt"):
            df = pd.read_csv(file_path, delimiter="|")
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path)
        elif file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            print(f"Skipped unsupported file: {file_path}")
            return

        # Add extra columns
        df['file_name'] = file_path
        df['user_name'] = 'pradip'
        df['rownumber'] = range(1, len(df) + 1)

        # Build SQL insert query
        column_names = df.columns.tolist()
        columns_str = ", ".join(f"`{col}`" for col in column_names)
        placeholders = ", ".join(["%s"] * len(column_names))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Insert rows
        records = [tuple(row) for row in df.values.tolist()]
        cursor.executemany(insert_sql, records)
        conn.commit()

        # Update timestamp correctly
        update_sql = f"UPDATE {table_name} SET Last_updated_at = CURRENT_TIMESTAMP() WHERE file_name = %s"
        cursor.execute(update_sql, (file_path,))
        conn.commit()

        cursor.close()
        conn.close()

        print(f"Data loaded into {table_name} from {file_path}")

        # Move file to processed folder
        destination = "data_folder\\processed_data"
        os.makedirs(destination, exist_ok=True)
        shutil.copy(file_path, destination)
        os.remove(file_path)

        print(f"File {file_path} moved to processed folder")

    else:
        print(f"File not found: {file_path}")


# Pattern-based file loading
patterns = {
    'ecommerce_order_raw_table': 'data_folder\\unprocessed_data\\*.csv',
    'crm_customer_raw_table': 'data_folder\\unprocessed_data\\*.xlsx',
    'marketing_events_raw_table': 'data_folder\\unprocessed_data\\*.json',
    'support_tickets_raw_table': 'data_folder\\unprocessed_data\\*.txt'
}

for table, pattern in patterns.items():
    for file_path in glob.glob(pattern):
        load_file(table, file_path)
