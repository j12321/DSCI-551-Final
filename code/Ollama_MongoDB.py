import re
from pymongo import MongoClient
import ollama
import streamlit as st
import pandas as pd
from bson.json_util import loads as bson_loads

def mongodb_query(natural_language, data):
    if data == "Stolen Vehicles":
        prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.

        There are there collections in the database: 
        1. `stolen_vehicles`:
           - Fields:
             - vehicle_id (string) - e.g., "10"
             - vehicle_type (string) — e.g., "Trailer", "Boat Trailer", "Passenger Car", "Truck"
             - make_id (string) -e.g., "623"
             - model_year (int) — e.g., 2005, 2017, 2022
             - vehicle_desc (string)
             - color (string) — e.g., "Red", "White", "Blue", "Black"
             - date_stolen (date) — format: "MM/DD/YY", e.g., "1/1/22"
             - location_id (string) - e.g., "102"
        
        2. `locations`:
           - Fields:
             - location_id (string) - e.g., "102"
             - region (string) — e.g., "Northland", "Southland"
             - country (string) — e.g., "New Zealand"
             - population (int)
             - density (float)

        3. `make_details`:
           - Fields:
             - make_id (string) -e.g., "623"
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
        Natural Language: Find count of stolen vehicles grouped by make_name, sorted by most frequently stolen, top 5 only. Join with make_details.
        MongoDB: db.stolen_vehicles.aggregate([
                  {{
                    $lookup: {{
                      from: "make_details",
                      localField: "make_id",
                      foreignField: "make_id",
                      as: "make_info"
                    }}
                  }},
                  {{ $unwind: "$make_info" }},
                  {{
                    $group: {{
                      _id: "$make_info.make_name",
                      total_stolen: {{ $sum: 1 }}
                    }}
                  }},
                  {{ $sort: {{ total_stolen: -1 }} }},
                  {{ $limit: 5 }}
                ])

        Example 3:
        Natural Language: Insert a new stolen vehicle report to stolen_vehicles, where vehicle_id: "V123456",
                vehicle_type: "Trailer", make_id: "M001", model_year: 2023, vehicle_desc: "Heavy duty trailer", color: "Red", 
                date_stolen: new Date("2024-04-01"), location_id: "L005".
        MongoDB: db.stolen_vehicles.insertOne({{
                  vehicle_id: "V123456",
                  vehicle_type: "Trailer",
                  make_id: "M001",
                  model_year: 2023,
                  vehicle_desc: "Heavy duty trailer",
                  color: "Red",
                  date_stolen: new Date("2024-04-01"),
                  location_id: "L005"
                }})

        Now convert {natural_language} to MongoDB Query:
        """

    elif data == "Pixar Films":
        prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.

        There are four collections in the database:
        1. `academy`:
           - Fields:
             - film (string)
             - award_type (string)
             - status (string)

        2. `genres`:
            - Fields:
              - film (string)
              - category (string) - e.g., "Genre", "Subgenre"
              - value (string) - e.g., "Adventure", "Buddy Comedy"

        3. `pixar_films`:
            - Fields:
              - number (string)
              - film (string)
              - release_data (date) - format: "YYYY-MM-DD", e.g., "1995-11-22"
              - run_time (int)
              - film_rating (string)
              - plot (string)

        4. `public_response`:
            - Fields:
              - film (string)
              - rotten_tomatoes_score (int)
              - rotten_tomatoes_counts (int)
              - metacritic_score (int)
              - metacritic_counts (int)
              - cinema_score (string)
              - imdb_score (float)
              - imdb_counts (int)

        Relationships:
            - `academy.film` joins with `genres.film`
            - `academy.film` joins with `pixar_films.film`
            - `academy.film` joins with `public_response.film`
            - `genres.film` joins with `pixar_films.film`
            - `genres.film` joins with `public_response.film`
            - `pixar_films.film` joins with `public_response.film`

        Query rules:
            - Use `.find()` for filtering documents
            - Use `.aggregate()` for joins, grouping, or projections
              EX: use aggregate() with `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$lookup`
            - Use `insert_one`, `insert_many`, `update_one`, `update_many`, `delete_one`, `delete_many`, for data modification
            - Output only one valid MongoDB query code, no comments or explanations

        Example 1.
        Natural Language: List all awards given to Pixar films from academy collection
        MongoDB query: db.academy.distinct("award_type")

        Example 2.
        Natural Language: List all films in the genre Adventure from genres collection
        MongoDB query: db.genres.find({{"value": "Adventure"}}, {{"film": 1, "_id": 0}})

        Example 3.
        Natural Language: List all films with Rotten Tomatoes score above 90 from public_response collection
        MongoDB query: db.public_response.find({{"rotten_tomatoes_score": {{"$gt": 90}}}}, {{"film": 1, "_id": 0}})

        Example 4.
        Natural Language: List films that won the Best Animated Feature award from academy collection
        MongoDB query: db.academy.find({{"award_type": "Animated Feature", "status": "Won"}}, {{"film": 1, "_id": 0}})

        Example 5.
        Natural Language: Show top 3 films by Rotten Tomatoes score with only film and score from public_response collection
        MongoDB query: db.public_response.find({{}}, {{"film": 1, "rotten_tomatoes_score": 1, "_id": 0}}).sort("rotten_tomatoes_score", -1).limit(3)

        Example 6.
        Natural Language: Skip the first 5 films and list the next 3 from pixar_films collection
        MongoDB query: db.pixar_films.find({{}}, {{"film": 1, "_id": 0}}).skip(5).limit(3)

        Example 7.
        Natural Language: Insert a new rating record of New Movie, with release date = July 01, 2025, run time = 110, film rating = PG, plot = dventure in the future to pixar_films collection.
        MongoDB query: db.pixar_films.insertOne({{"film": "New Movie", "release_date": "2025/07/01", "run_time": 110, "film_rating": "PG", "plot": "Adventure in the future."}})

        Example 8.
        Natural Language: Delete records of nominations with unknown status from academy collection
        MongoDB query: db.academy.deleteMany({{"status": {{"$exists": false}}}})

        Example 9.
        Natural Language: Update the Rotten Tomatoes score of Up to 100 from public_response collection
        MongoDB query: db.public_response.updateOne({{"film": "Up"}}, {{"$set": {{"rotten_tomatoes_score": 100}}}})

        Example 10.
        Natural Language: Count how many times the genre Comedy appears from genres collection
        MongoDB query: db.genres.aggregate([{{"$match": {{"value": "Comedy"}}}}, {{"$count": "comedy_count"}}])

        Example 11.
        Natural Language: List all films sorted by release date from pixar_films collections
        MongoDB query: db.pixar_films.find({{}}, {{"film": 1, "release_date": 1, "_id": 0}}).sort("release_date", 1)

        Example 12.
        Natural Language: Join pixar_films with academy to show which films won awards 
        MongoDB query: db.pixar_films.aggregate([
              {{
                $lookup: {{
                  from: "academy",
                  localField: "film",
                  foreignField: "film",
                  as: "awards"
                }}
              }},
              {{ $unwind: "$awards" }},
              {{ $match: {{ "awards.status": "Won" }} }},
              {{
                $project: {{
                  film: 1,
                  award_type: "$awards.award_type",
                  status: "$awards.status",
                  release_data: 1,
                  _id: 0
                }}
              }}
            ])

        Now convert {natural_language} to MongoDB Query:
        """

    elif data == "Beers":
        prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.

        There are four collections in the database:
        1. `Beers`
            - Fields:
              - name (string)
              - manf (string)

        2. `Frequents`
            - Fields:
              - drinker (string)
              - bar (string)

        3. `Likes`
            - Fields:
              - drinker (string)
              - beer (string)

        4. `Sells`
            - Fields:
              - bar (string)
              - beer (string)
              - price (int)

        Relationships:
            - `Beers.name` joins with `Likes.beer`
            - `Beers.name` joins with `Sells.beer`
            - `Frequents.bar` joins with `Sells.bar`
            - `Frequents.drinker` joins with `Likes.drinker`
            
        Query rules:
            - Use `.find()` for filtering documents
            - Use `.aggregate()` for joins, grouping, or projections
            - Output only one valid MongoDB query code, no comments or explanations

        Example 1:
        Natural Language: Find all beers named "Bud"
        MongoDB: db.Beers.find({{ name: "Bud" }})

        Example 2:
        Natural Language: Get the list of beers liked by "Steve"
        MongoDB: db.Likes.aggregate([
              {{ $match: {{ drinker: "Bob" }} }},
              {{ $project: {{ beer: 1, _id: 0 }} }}
            ])

        Example 3:
        Natural Language: Find the average price of each beer from Sells collection
        MongoDB: db.Sells.aggregate([
              {{ $group: {{ _id: "$beer", avgPrice: {{ $avg: "$price" }} }} }}
            ])

        Example 4:
        Natural Language: Join Likes with Beers
        MongoDB: db.Likes.aggregate([
              {{
                $lookup: {{
                  from: "Beers",
                  localField: "beer",
                  foreignField: "name",
                  as: "beerDetails"
                }}
              }}
            ])

        Example 5:
        Natural Language: Insert a new beer to Beers collection, where name = "Lagunitas IPA", manf = "Lagunitas Brewing Co"
        MongoDB: db.Beers.insertOne({{ name: "Lagunitas IPA", manf: "Lagunitas Brewing Co" }})

        Now convert {natural_language} to MongoDB Query:
        """
 
    response = ollama.chat(
        model='mistral:7b-instruct',
        messages=[{"role": "user", "content": prompt}],
        options={"timeout": 300}
    )
    result = response['message']['content']

    return result

# Connect to MongoDB
def connect_to_mongodb(data):
    client = MongoClient("mongodb://3.147.78.1:27017")
    if data == "Stolen Vehicles":
    	db = client['stolen_vehicles']
    elif data == "Pixar Films":
    	db = client['pixar_films']
    elif data == "Beers":
    	db = client['beer']
    return db

def safe_bson_loads(json_str):
    try:
        return bson_loads(json_str)
    except Exception as e:
        raise ValueError(f"BSON parsing error: {e}")

# Execute MongoDB Query String
def run_mongo_query(mongo_query, db):
    # Print Query (debug)
    print("[MongoDB Query Executed]")
    print(mongo_query)

    # Preprocess: extract clean MongoDB query from multi-line model output
    query_lines = re.findall(r'^db\..+', mongo_query, re.MULTILINE)
    if query_lines:
        mongo_query = "\n".join(query_lines).strip()

    try:
        if mongo_query.find(".find(") != -1:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.find')]
            query_body = mongo_query.split("find(")[-1].rsplit(")", 1)[0].strip()
            query_body = query_body.replace("'", '"')  # normalize quotes  
            try:
                matches = re.findall(r'\{.*?\}(?=\s*,|\s*$)', query_body)
                filter_part = matches[0] if len(matches) > 0 else "{}"
                projection_part = matches[1] if len(matches) > 1 else None
                filter_part = safe_bson_loads(filter_part.strip())
                if projection_part:
                    projection_part = safe_bson_loads(projection_part.strip())
                    result = db[collection].find(filter_part, projection_part)
                else:
                    result = db[collection].find(filter_part)
                return list(result)
            except Exception as e:
                return [f"Error parsing find() query: {e}"]
        elif ".insertOne(" in mongo_query:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.insertOne')]
            body = mongo_query.split("insertOne(")[-1].rstrip(")")
            quoted = safe_bson_loads(body.replace("'", '"'))
            result = db[collection].insert_one(quoted)
            return [f"Inserted ID: {result.inserted_id}"]
        elif ".insertMany(" in mongo_query:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.insertMany')]
            body = mongo_query.split("insertMany(")[-1].rstrip(")")
            quoted = safe_bson_loads(body.replace("'", '"'))
            result = db[collection].insert_many(quoted)
            return [f"Inserted IDs: {result.inserted_ids}"]
        elif ".updateOne(" in mongo_query:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.updateOne')]
            body = mongo_query.split("updateOne(")[-1].rstrip(")")
            filter_part, update_part = body.split(",", 1)
            result = db[collection].update_one(safe_bson_loads(filter_part), safe_bson_loads(update_part))
            return [f"Matched: {result.matched_count}, Modified: {result.modified_count}"]
        elif ".updateMany(" in mongo_query:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.updateMany')]
            body = mongo_query.split("updateMany(")[-1].rstrip(")")
            filter_part, update_part = body.split(",", 1)
            result = db[collection].update_many(safe_bson_loads(filter_part), safe_bson_loads(update_part))
            return [f"Matched: {result.matched_count}, Modified: {result.modified_count}"]
        elif ".deleteOne(" in mongo_query:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.deleteOne')]
            body = mongo_query.split("deleteOne(")[-1].rstrip(")")
            result = db[collection].delete_one(safe_bson_loads(body))
            return [f"Deleted Count: {result.deleted_count}"]
        elif ".deleteMany(" in mongo_query:
            collection = mongo_query[mongo_query.find('db.')+3: mongo_query.find('.deleteMany')]
            body = mongo_query.split("deleteMany(")[-1].rstrip(")")
            result = db[collection].delete_many(safe_bson_loads(body))
            return [f"Deleted Count: {result.deleted_count}"]
        else:
            return [f"Unsupported or unrecognized MongoDB operation:\n{mongo_query}"]
    except Exception as e:
        return [f"Error running query: {e}"]
    
# Streamlit UI
st.set_page_config(page_title="ChatDB: Natural Language Interface", layout="centered")
st.title("💬 ChatDB – Natural Language to Database Query")

user_input = st.text_area("📝 Enter your natural language query")

db_type = st.selectbox("🔍 Choose a database type", ["SQL", "MongoDB"])
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
                if query:
                    st.write("🛠️ Executing the following query on MongoDB:")
                    st.code(query)

                    db = connect_to_mongodb(data)
                    if db is not None:
                        try:
                            results = run_mongo_query(query, db)
                            if results:
                                # JSON Format
                                st.json(results)
                        except Exception as e:
                            st.error(f"❌ Error executing MongoDB query: {e}")
