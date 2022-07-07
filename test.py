import sqlalchemy
import sqlite3
import os

DATABASE_LOCATION = "sqlite:///crypto_exchanges_data.sqlite"

if __name__ == '__main__':

    conn = sqlite3.connect('crypto_exchanges_data.sqlite')
    cursor = conn.cursor()

    query = """
    SELECT id,trade_volume_24h_btc FROM crypto_exchange_overall_data
    """

    cursor.execute(query)
    print(cursor.fetchall())
    conn.close()
