import re
import textwrap
from pymongo import MongoClient
from ollama import chat

# Connect to MongoDB
def connect_to_mongodb(db_name):
    client = MongoClient("mongodb://3.147.86.250:27017")
    if db_name == "stolen_cars":
        db = client['stolen_vehicles']
    elif db_name == "pixar_films":
        db = client['pixar_films']
    elif db_name == "beers":
        db = client['beer']
    else:
        raise ValueError("Unknown db_name.")
    return db

SCHEMA = textwrap.dedent("""
Pixar MongoDB Collections and Fields:

    1. `academy`:
        - film (string)
        - award_type (string)
        - status (string)

    2. `genres`:
        - film (string)
        - category (string) - e.g., "Genre", "Subgenre"
        - value (string) - e.g., "Adventure", "Buddy Comedy"

    3. `pixar_films`:
        - number (string)
        - film (string)
        - release_date (date) - format: "YYYY/MM/DD", e.g., "1995/11/22"
        - run_time (int)
        - film_rating (string)
        - plot (string)

    4. `public_response`:
        - film (string)
        - rotten_tomatoes_score (int)
        - rotten_tomatoes_counts (int)
        - metacritic_score (int)
        - metacritic_counts (int)
        - cinema_score (string)
        - imdb_score (float)
        - imdb_counts (int)
""")

RULES = textwrap.dedent("""
- Output valid Python 3 code only (no explanations).
- Use the variable `client = MongoClient(MONGO_URI)` and `db = client[db_name]` to access the database.
- Always assign the collection using: `collection = db[collection_name]`
- Supported query forms:
  - `find(...)`, `find(...).sort(...)`, `find(...).limit(...)`, `find(...).skip(...)`
  - `aggregate([...])` with `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$lookup`
  - Data modification: `insert_one`, `insert_many`, `update_one`, `delete_one`
- Always assign final result to a variable named `result`
- Always format output as: `result = db.collection.query(...)`
- Do not include prompts, explanations, or unrelated comments.
""")

EXAMPLES = textwrap.dedent("""
1.
db: pixar_films
collection: academy
natural_language: List all awards given to Pixar films
MongoDB query:
result = db.academy.find({}, {"film": 1, "award_type": 1, "_id": 0})

2.
db: pixar_films
collection: genres
natural_language: List all films in the genre Adventure
MongoDB query:
result = db.genres.find({"value": "Adventure"}, {"film": 1, "_id": 0})

3.
db: pixar_films
collection: public_response
natural_language: List all films with Rotten Tomatoes score above 90
MongoDB query:
result = db.public_response.find({"rotten_tomatoes_score": {"$gt": 90}}, {"film": 1, "_id": 0})

4.
db: pixar_films
collection: academy
natural_language: List films that won the Best Animated Feature award
MongoDB query:
result = db.academy.find({"award_type": "Animated Feature", "status": "Winner"}, {"film": 1, "_id": 0})

5.
db: pixar_films
collection: public_response
natural_language: Show top 3 films by Rotten Tomatoes score with only film and score
MongoDB query:
result = db.public_response.find({}, {"film": 1, "rotten_tomatoes_score": 1, "_id": 0}).sort("rotten_tomatoes_score", -1).limit(3)

6.
db: pixar_films
collection: pixar_films
natural_language: Skip the first 5 films and list the next 3
MongoDB query:
result = db.pixar_films.find({}, {"film": 1, "_id": 0}).skip(5).limit(3)

7.
db: pixar_films
collection: pixar_films
natural_language: Insert a new rating record of New Movie, with release date = July 01, 2025, run time = 110, film rating = PG, plot = dventure in the future.
MongoDB query:
result = db.pixar_films.insert_one({"film": "New Movie", "release_date": "2025/07/01", "run_time": 110, "film_rating": "PG", "plot": "Adventure in the future."})

8.
db: pixar_films
collection: academy
natural_language: Delete records of nominations with unknown status
MongoDB query:
result = db.academy.delete_many({"status": {"$exists": False}})

9.
db: pixar_films
collection: public_response
natural_language: Update the Rotten Tomatoes score of Up to 100
MongoDB query:
result = db.public_response.update_one({"film": "Up"}, {"$set": {"rotten_tomatoes_score": 100}})

10.
db: pixar_films
collection: genres
natural_language: Count how many times the genre Comedy appears
MongoDB query:
result = db.genres.aggregate([{"$match": {"value": "Comedy"}}, {"$count": "comedy_count"}])

11.
db: pixar_films
collection: pixar_films
natural_language: List all films sorted by release date
MongoDB query:
result = db.pixar_films.find({}, {"film": 1, "release_date": 1, "_id": 0}).sort("release_date", 1)
""")

def generate_prompt(db_name, collection, nl_query):
    return f"""
You are a MongoDB expert.
Given a natural language question, output only the valid Python code that executes the corresponding MongoDB query.
{SCHEMA}
{RULES}
Examples:
{EXAMPLES}
Now answer this:
db_name: {db_name}
collection: {collection}
natural_language: {nl_query}
MongoDB query:
"""

def generate_mongo_query(prompt):
    response = chat(
        model="mistral",
        messages=[{"role": "user", "content": prompt}]
    )
    return response['message']['content']

def clean_code(code):
    code = code.strip()
    pattern = r'result\s*=\s*db\..+?(?:\n\S|\Z)'  # Match `result = db.collection...` block
    match = re.search(pattern, code, re.DOTALL)
    if match:
        return match.group(0).strip()
    return "result = None"

def execute_code(code, db_name, collection):
    try:
        db = connect_to_mongodb(db_name)
        col = db[collection]
        local_vars = {"db": db, "collection": col}
        print("\n>>> Final Code to Execute:")
        print(code)
        exec(code, {}, local_vars)
        return local_vars.get("result")
    except Exception as e:
        return f"[Execution Error] {e}"

if __name__ == "__main__":
    db_name = input("Which dataset? (pixar_films, stolen_cars, beers): ")
    collection = input("Which collection? (academy, genres, pixar_films, public_response): ")
    nl_query = input("Enter your query: ")

    prompt = generate_prompt(db_name, collection, nl_query)
    raw_code = generate_mongo_query(prompt)

    print("\n--- Raw Model Response ---")
    print(raw_code)

    final_code = clean_code(raw_code)
    print("\n--- Executing Code ---")
    print(final_code)

    result = execute_code(final_code, db_name, collection)
    print("\n--- Result ---")
    print(result)