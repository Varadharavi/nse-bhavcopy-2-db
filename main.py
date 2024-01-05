import os
import pandas as pd
import requests
from pymongo import MongoClient
from datetime import datetime
# Load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()
# Get the current date
current_date = datetime.now()
# Format the date as ddMMyyyy
formatted_date = current_date.strftime("%d%m%Y")
# NSE Bhav Copy Archive URL
nse_bhav_url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_' + formatted_date + '.csv'
save_path = os.path.join(os.getcwd(), 'NSE_Data', nse_bhav_url.split("/")[-1])
mongo_uri = os.environ.get('MONGO_URI')
db_name = os.environ.get('DB_NAME')
collection_name = os.environ.get('COLLECTION_NAME')


def download_csv(url, save_path):
    response = requests.get(url)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"CSV file downloaded successfully and saved at {save_path}")
        insert_data_2_db()
        print(f"Data Inserted into DB")
    else:
        print(f"Failed to download CSV. Status code: {response.status_code}")


def open_mongodb_connection():
    # Connect to the MongoDB server
    client = MongoClient(mongo_uri)  # Replace with your MongoDB connection string
    # Select the database and collection
    db = client[db_name]
    collection = db[collection_name]
    return db, collection


def close_mongodb_connection(client):
    client.close()


def insert_data_2_db():
    db, collection = open_mongodb_connection()
    df = pd.read_csv(save_path, parse_dates=[" DATE1"])
    df.columns = df.columns.str.replace(' ', '')
    # Remove leading and trailing spaces from all data
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df['LAST_PRICE'] = pd.to_numeric(df['LAST_PRICE'], errors='coerce')
    df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
    # Convert DataFrame to list of dictionaries
    data = df.to_dict(orient="records")
    # Insert data into MongoDB
    result = collection.insert_many(data)
    print(f"Inserted {len(result.inserted_ids)} documents")
    close_mongodb_connection(db.client)


download_csv(nse_bhav_url, save_path)

