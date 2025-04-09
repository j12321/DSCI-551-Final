from huggingface_hub import InferenceClient
import pandas as pd
import sqlalchemy
import pymysql
import requests
import json
import sys


client = InferenceClient(
   model="tiiuae/falcon-7b-instruct",
   token="ypur_token",  # replace with your hugging face api token
)

def sql_query(natural_language):
    result = client.text_generation(
       prompt = f"Convert this request into a SQL query:\n\"{natural_language}\"",
        max_new_tokens=100,
        temperature=0.2,
    )
    return result

def mongodb_query(natural_language):
    result = client.text_generation(
       prompt = f"Convert this request into a MongoDB query:\n\"{natural_language}\"",
        max_new_tokens=100,
        temperature=0.2,
    )
    return result

def firebase_query(natural_language):
    result = client.text_generation(
       prompt = f"Convert this request into a Firebase Realtime Database code in Python:\n\"{natural_language}\"",
        max_new_tokens=100,
        temperature=0.2,
    )
    return result

# MySQL
sql_q = sql_query("Find the names of all employees who earn more than $50,000")
sql_q = sql_q.strip('\n')
print(sql_q)

# MongoDB
mongo_q = mongodb_query("Find the names of all employees who earn more than $50,000")
print(mongo_q)

# Firebase
fire_q = firebase_query("Find the names of all employees who earn more than $50,000")
print(fire_q)