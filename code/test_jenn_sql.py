import streamlit as st
from huggingface_hub import InferenceClient
import sqlalchemy
import pandas as pd
import re
from pymongo import MongoClient
from bson import SON
from bson.json_util import loads as bson_loads

# Hugging Face setup
client = InferenceClient(
   model="mistralai/Mistral-7B-Instruct-v0.1",
   token="your api"  # Replace with your actual Hugging Face token
)

# Hugging Face query converters
def sql_query(natural_language):
    result = client.text_generation(
       prompt=f"Convert this request into a SQL query:\n\"{natural_language}\"",
       max_new_tokens=100,
       temperature=0.2,
    )
    return result

def mongodb_query(natural_language, data):
    try:
        if data == "stolen cars":
            prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.
            
            There are there tables in the database: 
            1. `stolen_vehicles`:
            - Fields:
                - vehicle_id (string)
                - vehicle_type (string) — e.g., "Trailer", "Boat Trailer", "Passenger Car", "Truck"
                - make_id (string)
                - model_year (int) — e.g., 2005, 2017, 2022
                - vehicle_desc (string)
                - color (string) — e.g., "Red", "White", "Blue", "Black"
                - date_stolen (date) — format: "MM/DD/YY", e.g., "1/1/22"
                - location_id (string)
            
            2. `locations`:
            - Fields:
                - location_id (string)
                - region (string) — e.g., "Northland", "Southland"
                - country (string) — e.g., "New Zealand"
                - population (int)
                - density (float)

            3. `make_details`:
            - Fields:
                - make_id (string)
                - make_name (string)
                - make_type (string) — e.g., "Standard", "Luxury"
            
            Relationships:
                - `stolen_vehicles.make_id` joins with `make_details.make_id`
                - `stolen_vehicles.location_id` joins with `locations.location_id`
                
            Query rules:
                - Use `.find()` for filtering documents
                - Use `.aggregate()` for joins, grouping, or projections
                - Output only one valid MongoDB query code, no comments or explanations
            
            Example 1:
            Natural Language: Find all stolen vehicles of type "Trailer" stolen after Jan 1, 2022.
            MongoDB: db.stolen_vehicles.find({{ vehicle_type: "Trailer", date_stolen: {{ $gt: "1/1/22" }} }})
            
            Example 2:
            Natural Language: For each stolen vehicle, find the region and country where the theft occurred by joining the vehicle's location_id with the corresponding location in the locations collection. 
            Then, display the vehicle's description, color, date it was stolen, along with the region and country.
            MongoDB: db.stolen_vehicles.aggregate([
                {{
                    $lookup: {{
                    from: "locations",
                    localField: "location_id",
                    foreignField: "location_id",
                    as: "location_info"
                    }}
                }},
                {{
                    $unwind: "$location_info"
                }},
                {{
                    $project: {{
                    vehicle_desc: 1,
                    color: 1,
                    date_stolen: 1,
                    region: "$location_info.region",
                    country: "$location_info.country"
                    }}
                }}
                ])

            Now convert: {natural_language}
            MongoDB:
            """
        elif data == "pixar film":
            prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.

            There are six tables in the database:
            1. `academy`:
            - Fields:
                - film (string)
                - award_type (string)
                - status (string)
                
            2. `box_office`:
                - Fields:
                - film (string)
                - budget (int)
                - box_office_us_canada (string)
                - box_office_other (string)
                - box_office_worldwide (string)

            3. `genres`:
                - Fields:
                - film (string)
                - category (string) - e.g., "Genre", "Subgenre"
                - value (string) - e.g., "Adventure", "Buddy Comedy"

            4. `pixar_films`:
                - Fields:
                - number (string)
                - film (string)
                - release_data (date) - format: "YYYY-MM-DD", e.g., "1995-11-22"
                - run_time (int)
                - film_rating (string)
                - plot (string)
            
            5. `pixar_people`:
                - Fields:
                - film (string)
                - role_type (string)
                - name (string)

            6. `public_response`:
                - Fields:
                - film (string)
                - rotten_tomatoes_score (int)
                - rotten_tomatoes_counts (int)
                - metacritic_score (int)
                - metacritic_counts (int)
                - cinema_score (string)
                - imdb_score (float)
                - imdb_counts (int)

            Query rules:
                - Use `.find()` for filtering documents
                - Use `.aggregate()` for joins, grouping, or projections
                - Output only one valid MongoDB query code, no comments or explanations
            """
        
        elif data == "airbnb":
            prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.

            There are two tables in the database:
            1. `Listings`

            2. `Reviews`
            """
        result = client.text_generation(
            prompt=prompt,
            temperature=0.2,
            )
        return result
    except:
        return "Unsupported dataset."
    
    

def firebase_query(natural_language):
    result = client.text_generation(
       prompt=f"Convert this request into a Firebase Realtime Database code in Python:\n\"{natural_language}\"",
       max_new_tokens=100,
       temperature=0.2,
    )
    return result

# DELETE??????
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


# Connect to MongoDB
def connect_to_mongodb():
    client = MongoClient()
    db = client['final']
    return db

# Execute MongoDB Query String
def run_mongo_query(mongo_query, db):
    mongo_query = mongo_query[mongo_query.find('db.'): mongo_query.find(')')+1]
    
    try:
        if mongo_query.find(".find(") != -1:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.find')]
            query_body = mongo_query.split("find(")[-1].rstrip(")")
            quoted = re.sub(
                r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)(\s*:)',
                r'\1"\2"\3',
                query_body
                )
            quoted = re.sub(r'(?<!")(\$\w+)', r'"\1"', quoted)
            quoted = quoted.replace("'", '"')
            quoted = bson_loads(quoted)
            result = db[collection].find(quoted)
            return list(result)
        elif mongo_query.find(".aggregate(") != -1:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.aggregate')]
            query_body = mongo_query.split("aggregate(")[-1].rstrip(")")
            quoted = re.sub(
                r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)(\s*:)',
                r'\1"\2"\3',
                query_body
                )
            quoted = re.sub(r'(?<!")(\$\w+)', r'"\1"', quoted)
            quoted = quoted.replace("'", '"')
            quoted = bson_loads(quoted)
            result = db[collection].aggregate(quoted)
            return list(result)
        else:
            return ["Unsupported query type."]
    except Exception as e:
        return [f"Error running query: {e}"]
    

# Streamlit UI
st.set_page_config(page_title="ChatDB: Natural Language Interface", layout="centered")
st.title("💬 ChatDB – Natural Language to Database Query")

user_input = st.text_area("📝 Enter your natural language query")

db_type = st.selectbox("🔍 Choose a database type", ["SQL", "MongoDB", "Firebase"])
execute_query = st.checkbox("⚙️ Execute the query")
data = st.selectbox("Choose which dataset to access:", ["Pixar Films", "Stolen Vehicles", "Beers"])

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
                        if data == 'Pixar Films':
                            engine = get_mysql_engine('pixar_movies')
                        elif data == 'Stolen Vehicles':
                            engine = get_mysql_engine('stolen_vehicles')
                        elif data == 'Beers':
                            engine = get_mysql_engine('beers')
                        if engine:
                            result = execute_sql(engine, query)
                            if isinstance(result, pd.DataFrame):
                                st.dataframe(result)
                            else:
                                st.write(result)
                else:
                    st.warning("⚠️ Could not extract database name from the query.")
            elif db_type == "MongoDB":
                query = mongodb_query(user_input, data).strip()
                st.subheader("🛠️ Generated MongoDB Query")
                st.code(query)

                db = connect_to_mongodb()
                if db:
                    results = run_mongo_query(mongo_query, db)
                    st.write(results)
            elif db_type == "Firebase":
                query = firebase_query(user_input).strip()
                st.subheader("🛠️ Generated Firebase Python Code")
                st.code(query, language="python")
