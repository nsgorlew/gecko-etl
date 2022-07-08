import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
import sqlite3
import os

def valid_data_check(df: pd.DataFrame) -> bool:
	# Check for empty dataframe
	if df.empty:
		print("Cannot retrieve data from API")
		return False
	
	# Check for primary key duplicates
	if pd.Series(df['id']).is_unique:
		pass
	else:
		raise Exception("Primary Key duplicate found")

	# Check for nulls
	if df.isnull().values.any():
		raise Exception("Null values found")

	return True

def run_crypto_etl():

	database_location = "sqlite:///crypto_exchanges_data.sqlite"

	# Extract
	headers = {
		"Accept" : "application/json",
	}

	req = requests.get("https://api.coingecko.com/api/v3/exchanges",headers=headers)
	
	data = req.json()
	
	identifiers = []
	names  = []
	year_established = []
	has_trading_incentive = []
	trust_score = []
	trust_score_rank = []
	trade_volume_24h_btc = []
	trade_volume_24h_btc_normalized = []

	# Extract only relevant data
	for exchange in data:
		identifiers.append(exchange["id"])
		names.append(exchange["name"])
		if exchange["year_established"] is None:
			year_established.append(0)
		else:
			year_established.append(exchange["year_established"])
		if exchange["has_trading_incentive"] is None:
			has_trading_incentive.append(False)
		else:
			has_trading_incentive.append(exchange["has_trading_incentive"])
		trust_score.append(exchange["trust_score"])
		trust_score_rank.append(exchange["trust_score_rank"])
		trade_volume_24h_btc.append(exchange["trade_volume_24h_btc"])
		trade_volume_24h_btc_normalized.append(exchange["trade_volume_24h_btc_normalized"])

	# Dictionary for DataFrame
	exchange_dict = {
		"id" : identifiers,
		"name" : names,
		"year_established" : year_established,
		"trading_incentive" : has_trading_incentive,
		"trust_score" : trust_score,
		"trust_score_rank" : trust_score_rank,
		"trade_volume_24h_btc" : trade_volume_24h_btc,
		"trade_volume_24h_btc_normalized" : trade_volume_24h_btc_normalized
	}

	# Transform Stage
	exchange_df = pd.DataFrame(exchange_dict, columns = ["id","name","year_established","trading_incentive","trust_score","trust_score_rank","trade_volume_24h_btc","trade_volume_24h_btc_normalized"])
	print(exchange_df.isnull().sum())

	# Validate the df
	if valid_data_check(exchange_df):
		print("Data is valid, proceeding to Load stage...")

	# Load Stage
	engine = sqlalchemy.create_engine(database_location)
	conn = sqlite3.connect('crypto_exchanges_data.sqlite')
	cursor = conn.cursor()

	query = """
	CREATE TABLE IF NOT EXISTS crypto_exchange_overall_data(
		id VARCHAR(200),
		name VARCHAR(200),
		year_established INTEGER,
		trading_incentive INTEGER,
		trust_score INTEGER,
		trust_score_rank INTEGER,
		trade_volume_24h_btc REAL,
		trade_volume_24h_btc_normalized REAL
	)
	"""

	cursor.execute(query)
	conn.commit()
	print("Opened database successfully")

	try:
		exchange_df.to_sql("crypto_exchange_overall_data",engine,index=False, if_exists='append')
		print("Data from API successfully written to database")
	except:
		print("Data already exists in the database")

	conn.close()
	print("Database closed")