########################################################################################################################################
# Name: bitcoin_price_ranking_by_date.py
# Purpose: This script requests bitcoin price data and provides daily price rankings compared to prices from the previous 30 days.
# Created By: Terrance Daye
# Created Date: 04/09/2021
########################################################################################################################################

import os
import re
import sys
import glob
import string
import requests
import pandas as pd
import json
import datetime
from datetime import timedelta
from datetime import date

# Global variables
today = date.today()

# Process Bitcoin Price Data
def process_bitcoin_price_data():
    print("Executing get_bitcoin_price_data:")
    print ("")

    try:
        response = requests.get('https://api.coinranking.com/v1/public/coin/1/history/30d')

        if response.status_code == 200:
            print("Request Completed!")
            print ("")

            response_data = response.json()
            price_history_data = response_data['data']['history']

            format_price_history_data(price_history_data)

            # print("Price Data: \n", response_data['data']['history'])
            # print ("")
        else:
            print("An error occurred, no data is available.")
            print ("")
    except Exception as e:
        print ("Error: {0}".format(str(e)))
        print ("")


# Format Price History Data
def format_price_history_data(price_history_data):
    print("Executing format_price_history_data:")
    print ("")

    try:
        # Create DataFrame
        df1 = pd.DataFrame(price_history_data)

        # Format price values
        df1['price'] = df1['price'].astype(float)
        df1['price'] = df1.round({'price':2})

        # Format timestamp to ISO-8601
        df1['timestamp'] = (pd.to_datetime(df1['timestamp'],unit='ms')).dt.strftime('%Y-%m-%dT%H:%M%:%S')

        # Sort and filter data by timestamp
        filtered_rows = df1.where(cond=df1['timestamp'].str.contains('T00:00:00', na = False))
        df2 = pd.DataFrame(filtered_rows).dropna(how='all')
        df2.sort_values(by=['timestamp'], ascending = True)

        # Add new colums to DataFrame
        df2.insert(2, 'direction', 'na') 
        df2.insert(3, 'change', '')
        df2.insert(4, 'dayOfWeek', '')
        df2.insert(5, 'highSinceStart', '')
        df2.insert(6, 'lowSinceStart', '')

        # Identify all fields where the direction is 'up'
        df2['change'] = (df2.price > df2.price.shift())
        df2['direction'].mask(df2['change'] == True, 'up', inplace = True)

        # Identify all fields where the direction is 'down'
        df2['change'] = (df2.price < df2.price.shift())
        df2['direction'].mask(df2['change'] == True, 'down', inplace = True)

        # Identify all fields where the direction is 'same'
        df2['change'] = (df2.price == df2.price.shift())
        df2['direction'].mask(df2['change'] == True, 'same', inplace = True)

        # Calculate the amount of change in price
        df2['change'] = (df2.price - df2.price.shift())
        df2['change'].mask(pd.isna(df2['change']), 'na', inplace = True)

        # Identify the day of the week
        df2['dayOfWeek'] = pd.to_datetime(df1['timestamp']).dt.day_name()

        # Identify highest and lowest value since start
        df2['highSinceStart'] = (df2.price == df2['price'].max())
        df2['lowSinceStart'] = (df2.price == df2['price'].min())

        # Rename and Reorder dataframe columns
        df2.rename(columns = {'timestamp': 'date'}, inplace = True)
        column_names = ['date', 'price', 'direction', 'change', 'dayOfWeek', 'highSinceStart', 'lowSinceStart']
        df2 = df2.reindex(columns=column_names)

        json_records = df2.to_json(orient ='records')
        print("json_records = ", json_records, "\n")

        # Write data to a JSON file
        create_json_file(df2)

        # print("{} \n".format(df1))
        # print("{} \n".format(df2))
    except Exception as e:
        print ("Error: {0}".format(str(e)))
        print ("")


# Create JSON file
def create_json_file(data_frame):
    print("Executing create_json_file:")
    print ("")

    try:
        # Write dataframe to JSON file
        file_path = "./bitcoin_price_rankings_%s.json" % today.strftime("%Y%m%d")
        data_frame.to_json(file_path, orient ='records')

        print("JSON File Created!")
        print ("")
    except Exception as e:
        print ("Error: {0}".format(str(e)))
        print ("")


# Main Program
def main():
    # Retrieve price history data
    process_bitcoin_price_data()

#Execute Script
main()