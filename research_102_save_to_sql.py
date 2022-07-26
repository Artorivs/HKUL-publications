import sqlite3 as sql
import pandas as pd
import numpy as np
import os

# get all file names under hkul-publications/processed/research
files = sorted(os.listdir('./processed/research'))

# connect to sqlite3 database 'research-com.db'
conn = sql.connect('research-com.db')

# save all files to sql database
for file in files:
    # read file
    df = pd.read_csv('./processed/research/' + file, sep='\t')
    # create table
    df.to_sql(file.replace('.tsv', ''), con=sql.connect('research-com.db'), if_exists='append', index=False)
    print('Saved ' + file + ' to sql database')

# close connection
conn.close()
