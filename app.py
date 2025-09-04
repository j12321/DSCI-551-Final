import streamlit as st
from openai import OpenAI
from db_utils import run_mysql_query, run_mongo_query

# --- UI ---
st.title("DSCI551 Final Project - ChatDB")

db_type = st.selectbox("Choose a database type", ["MySQL", "MongoDB"])
dataset = st.selectbox("Choose a dataset", [
    "beers",
    "pixar_movies",
    "stolen_vehicles_db"
])

SCHEMAS = {
    "pixar_movies": {
        "pixar_films": ["film", "release_date", "run_time", "film_rating", "plot"],
        "box_office": ["film", "budget", "box_office_us_canada", "box_office_other", "box_office_worldwide"],
        "genres": ["film", "category", "value"],
        "pixar_people": ["film", "role_type", "name"],
        "public_response": [
            "film", "rotten_tomatoes_score", "rotten_tomatoes_counts",
            "metacritic_score", "metacritic_counts",
            "cinema_score", "imdb_score", "imdb_counts"
        ],
        "academy": ["film", "award_type", "status"]
    },
    "stolen_vehicles_db": {
        "stolen_vehicles": ["vehicle_id", "vehicle_type", "make_id", "model_year", "vehicle_desc", "color", "date_stolen", "location_id"],
        "make_details": ["make_id", "make_name", "make_type"],
        "locations": ["location_id", "region", "country", "population", "density"]
    },
    "beers": {
        "Beers": ["name", "manf"],
        "Bars": ["name", "addr"],
        "Drinkers": ["name", "addr", "phone"],
        "Sells": ["bar", "beer", "price"],
        "Likes": ["drinker", "beer"],
        "Frequents": ["drinker", "bar"]
    }
}

user_input = st.text_area("Ask your question")

if st.button("Generate & Run Query") and user_input.strip():
    with st.spinner("Generating query..."):
        schema = SCHEMAS[dataset]
        schema_text = "\n".join([f"{table}: {', '.join(cols)}" for table, cols in schema.items()])

        # MySQL prompt
        if db_type == 'MySQL':
            prompt = (
                f"You are an assistant that converts natural language into MySQL queries"
                f"using the Python Pymysql syntax. The user is working with MySQL Ver 8.0.41 with Pymysql library\n\n"

                f"The user will ask questions about the database named '{dataset}', which contains the following collections and fields:\n"
                f"{schema_text}\n\n"

                f"Guidelines:\n"
                f"1. Always use Pymysql syntax\n"
                f"2. Only return the sql query itself — do not include cursor.execute, markdown formatting, code blocks, quotation marks, or explanation.\n"
                f"3. Don't hardcode sample data — just write the query structure to retrieve or modify the database.\n\n"

                f"Natural language input: {user_input}"
            )

        # MongoDB prompt
        elif db_type == 'MongoDB':
            prompt = (
                f"You are an assistant that converts natural language into MongoDB queries"
                f"using the Python PyMongo syntax. The user is working with MongoDB 6.0 using the PyMongo library.\n\n"
    
                f"The user will ask questions about the database named '{dataset}', which contains the following collections and fields:\n"
                f"{schema_text}\n\n"
    
                f"Guidelines:\n"
                f"1. Always use PyMongo syntax — output Python-style queries, not raw Mongo shell or JavaScript.\n"
                f"2. All queries must start with `db.` followed by the collection name and method — for example: `db.genres.find()` or `db.genres.aggregate([...])`\n"
                f"3. Never use `db['collection']` syntax — only use `db.collection`.\n"
                f"4. Only return the valid Python expression — do not include markdown formatting, code blocks, or explanation.\n"
                f"5. Don't hardcode sample data — just write the query structure to retrieve or modify the database.\n\n"
    
                f"Natural language input: {user_input}"
            )

        # enter your OpenAI API Key
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "You convert natural language into database queries."},
                {"role": "user", "content": prompt}
            ]
        )

        query = response.choices[0].message.content.strip()

        if query.startswith("```"):
            query = "\n".join(query.split("\n")[1:-1]).strip()

    st.subheader("📋 Generated Query")
    st.code(query, language="sql" if db_type == "MySQL" else "python")

    with st.spinner("Running query on EC2..."):
        try:
            if db_type == "MySQL":
                results = run_mysql_query(query, dataset)
            else:
                results = run_mongo_query(query, dataset)

            st.subheader("✅ Results")
            st.dataframe(results)
        except Exception as e:
            st.error(f"❌ Error running query:\n{e}")
