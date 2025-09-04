import pandas as pd
import pymysql
from pymongo import MongoClient
from pymongo.cursor import Cursor

# --- MySQL Query Execution ---
def run_mysql_query(query, db_name):
    try:
        connection = pymysql.connect(
            host="your-ec2-ip",           # enter your ec2 ip
            user="your-mysql-username",   # enter your mysql username
            password="mysql-password",    # enter your mysql password
            db=db_name,
            port=3307
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            if cursor.description:  # SELECT query
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)
            else:  # INSERT, UPDATE, DELETE
                connection.commit()
                return pd.DataFrame([{"status": "✅ Query executed successfully"}])
    except Exception as e:
        return pd.DataFrame([{"error": str(e)}])

# --- MongoDB Query Execution ---
def run_mongo_query(query_string, dataset):
    try:
        client = MongoClient("mongodb://your-ec2-ip/")  # enter your ec2 ip
        db = client[dataset]  # dynamic DB selection

        if not query_string.startswith("db."):
            return pd.DataFrame([{"error": "Query must start with 'db.'"}])

        # Strip the "db." prefix and evaluate the rest
        raw_expr = query_string[3:].strip()

        result = eval(f"db.{raw_expr}")

        # Handle write ops like insert/update/delete separately
        if hasattr(result, 'acknowledged'):
            data = [{"acknowledged": result.acknowledged}]
            if hasattr(result, "inserted_id"):
                data[0]["inserted_id"] = str(result.inserted_id)
        elif isinstance(result, Cursor):
            data = list(result)
        elif isinstance(result, dict):
            data = [result]
        else:
            data = list(result)

        return pd.DataFrame(data)

    except Exception as e:
        return pd.DataFrame([{"error": str(e)}])
