import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
import datetime

def log_progress(message):
    date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("code_log.txt","a") as file:
        file.write(f"{date_time} : {message}\n")
        

def extract(url,headers):
    
    htmlpage = requests.get(url).text
    htmlData = BeautifulSoup(htmlpage, 'html.parser')
    table = htmlData.find(name = "table")
    rows = table.find_all(name = "tr")
    df = pd.DataFrame(columns = headers)
    
    for row in rows:
        col = row.find_all(name = "td")
        if len(col) != 0:
            bank_name = col[1].find_all(name = "a")[1]['title']
            market_cap = float(col[2].contents[0][:-1])
            
            data_dict = {"Name": bank_name,
                         "MC_US_Billion": market_cap
                }
            
            df1 = pd.DataFrame(data_dict,index = [0])
            df = pd.concat([df,df1],ignore_index = True)

    return df

def transform(df,exchange_rate_file):
    exchange_rate = pd.read_csv(exchange_rate_file)
    

    df["MC_GBP_Billion"] = (df["MC_US_Billion"]*exchange_rate.loc[1]["Rate"]).round(2)
    df["MC_EUR_Billion"] = (df["MC_US_Billion"]*exchange_rate.loc[0]["Rate"]).round(2)
    df["MC_INR_Billion"] = (df["MC_US_Billion"]*exchange_rate.loc[2]["Rate"]).round(2)
    
    return df

def load_to_csv(df,output_filename):
    df.to_csv(output_filename)

def load_to_db(sql_connection,table_name,df):
    df.to_sql(table_name, sql_connection, if_exists = "replace", index = False)

def run_query(query_statement,sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)
    
exchange_rate_file = "exchange_rate.csv"
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
headers = ["Name", 
           "MC_US_Billion"]
output_filename = "Largest_banks_data.csv"

table_name = "Largest_banks"
database_name = "Banks.db"

log_progress("Preliminaries complete. Initiating ETL process.")

df = extract(url,headers)
log_progress("Data Extraction conmplete. Initiating Transformation process")

df = transform(df,exchange_rate_file)
log_progress("Data Transformation conmplete. Initiating Loading process")

load_to_csv(df,output_filename)
log_progress("Data saved to CSV file")

log_progress("SQL Connection Initiated")
conn = sqlite3.connect(database_name)
load_to_db(conn,table_name,df)
log_progress("Data Loaded to Database as a table, Executing queries")

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement,conn)

query_statement = f"SELECT AVG(MC_GBP_BILLION) FROM {table_name}"
run_query(query_statement,conn)

query_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement,conn)


log_progress("Process Complete")
log_progress("Server Connection closed")


