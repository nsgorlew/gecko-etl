import requests, json, asyncio
from flask import Flask, flash, jsonify, request, redirect, url_for, render_template
import sqlite3
import pandas as pd

app = Flask(__name__)

@app.route('/',methods=['POST','GET'])
def home():
	conn = sqlite3.connect('crypto_exchanges_data.sqlite')
	df = pd.read_sql_query("SELECT id,trade_volume_24h_btc FROM crypto_exchange_overall_data WHERE trade_volume_24h_btc > 70000",conn,index_col=None)
	df = df.reset_index()

	# create array of dictionaries...each dictionary is one crypto exchange
	exchanges = []
	volumes = []
	for index,row in df.iterrows():
		exchanges.append(row["id"])
		volumes.append(row["trade_volume_24h_btc"])
	return render_template('index.html',exchanges=exchanges,volumes=volumes)

if __name__ == '__main__':
	app.run()