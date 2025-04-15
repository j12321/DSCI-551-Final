import streamlit as st
from huggingface_hub import InferenceClient
import sqlalchemy
import pandas as pd
import re

# Hugging Face setup
client = InferenceClient(
   model="mistralai/Mistral-7B-Instruct-v0.1",
   token="your token here"  # Replace with your actual Hugging Face token
)

# Hugging Face query converters
def sql_query(natural_language):
    result = client.text_generation(
       prompt=f"Convert this request into a SQL query:\n\"{natural_language}\"",
       max_new_tokens=100,
       temperature=0.2,
    )
    return result

def mongodb_query(natural_language):
    result = client.text_generation(
       prompt=f"Convert this request into a MongoDB query:\n\"{natural_language}\"",
       max_new_tokens=100,
       temperature=0.2,
    )
    return result

def firebase_query(natural_language):
    result = client.text_generation(
       prompt=f"Convert this request into a Firebase Realtime Database code in Python:\n\"{natural_language}\"",
       max_new_tokens=100,
       temperature=0.2,
    )
    return result

# Extract DB name from SQL like: FROM db.table or JOIN db.table
def extract_db_name(sql_text):
    matches = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z_][\w]*)\.', sql_text, re.IGNORECASE)
    return matches[0] if matches else None

# Connect to dynamic MySQL database
def get_mysql_engine(db_name):
    try:
        engine = sqlalchemy.create_engine(f'mysql+mysqldb://root:Dsci-551@localhost/{db_name}')
        return engine
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        return None

# Run SQL query
def execute_sql(engine, query):
    try:
        with engine.connect() as connection:
            if query.strip().lower().startswith("select"):
                df = pd.read_sql_query(query, connection)
                return df
            else:
                connection.execute(sqlalchemy.text(query))
                return "✅ Query executed successfully."
    except Exception as e:
        return f"❌ SQL execution error: {e}"

# Streamlit UI
st.set_page_config(page_title="ChatDB: Natural Language Interface", layout="centered")
st.title("💬 ChatDB – Natural Language to Database Query")

user_input = st.text_area("📝 Enter your natural language query")

db_type = st.selectbox("🔍 Choose a database type", ["SQL", "MongoDB", "Firebase"])
execute_query = st.checkbox("⚙️ Execute the query (for SQL only)")

if st.button("Generate"):
    if not user_input.strip():
        st.warning("Please enter a query.")
    else:
        with st.spinner("Translating..."):
            if db_type == "SQL":
                query = sql_query(user_input).strip()
                st.subheader("🛠️ Generated SQL")
                st.code(query, language="sql")

                db_name = extract_db_name(query)
                if db_name:
                    st.write(f"📁 Detected Database: `{db_name}`")
                    if execute_query:
                        engine = get_mysql_engine(db_name)
                        if engine:
                            result = execute_sql(engine, query)
                            if isinstance(result, pd.DataFrame):
                                st.dataframe(result)
                            else:
                                st.write(result)
                else:
                    st.warning("⚠️ Could not extract database name from the query.")
            elif db_type == "MongoDB":
                query = mongodb_query(user_input).strip()
                st.subheader("🛠️ Generated MongoDB Query")
                st.code(query)
            elif db_type == "Firebase":
                query = firebase_query(user_input).strip()
                st.subheader("🛠️ Generated Firebase Python Code")
                st.code(query, language="python")
