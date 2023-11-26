# import python libraries
import pymysql
import pandas as pd
import numpy as np



parse_dates = ['date_added']
netflix_titles = pd.read_csv('mycsvfile.csv', parse_dates=parse_dates) # using pandas to read the csv file 
# Preprocessing netflix_titles DataFrame
netflix_titles['date_added'] = pd.to_datetime(netflix_titles['date_added'], errors='coerce')
netflix_titles['date_added'] = netflix_titles['date_added'].fillna(pd.to_datetime('1900-01-01'))
netflix_titles['date_added'] = netflix_titles['date_added'].dt.strftime("%Y-%m-%d")
fullList = netflix_titles['title'].values.tolist()
genreList = list(set(netflix_titles['listed_in'].values.tolist())) # genre data for genre_dim table
showDim = netflix_titles.drop(['date_added', 'release_year', 'rating', 'listed_in'], axis=1) # delete the columns that are not needed
showDim = showDim.where((pd.notnull(showDim)), None) # convert empty values to "None" values
showDimList = []
for row in showDim.values.tolist():
    showDimList.append(tuple(row)) # format data into a list of tuples before inserting to database


# showDimList: show data for show_dim table
netflix_origionals = pd.read_csv('netflix_originals.csv') # using pandas to read the csv file
netflix_origionals = netflix_origionals[['Title','Seasons','Length','Netflix Exclusive Regions','Status']] # select the columns to keep
netflix_origionals = netflix_origionals.where((pd.notnull(netflix_origionals)), None) # convert empty values to "None" values
netflix_origionals.drop_duplicates(subset ="Title", 
                     keep = 'first', inplace = True) # delete duplicate values 
netflix_origionalsList = [tuple(l) for l in netflix_origionals.values.tolist()] # format data into a list of tuples before inserting to database
#print(netflix_origionalsList) #: data for original_dim table
netflix_stocks = pd.read_csv('NFLX.csv', parse_dates=['Date']) # using pandas to read the csv file
dateDim = pd.DataFrame({'date': pd.date_range(start='2002-05-23', end='2020-08-03')}) # create a dataframe that has dates ranging from 2002-05-23 to 2020-08-03
dateDim['date_id'] = dateDim.index + 1 # create date_id column and assign id numbers starting from 1
dateDim['date'] = dateDim['date'].dt.strftime("%Y-%m-%d") # format date column to YYYY-MM-DD
dateDim['year'] = pd.DatetimeIndex(dateDim['date']).year # using the year information from date colunn to create year column
dateDim = dateDim.reindex(columns=['date_id','date','year']) # re-arrange the order of columns
#dateDim.date = pd.to_datetime(dateDim.date)
dateDim.date_id = dateDim.date_id.astype(str) # convert data in date column to string
dateDimList = [tuple(l) for l in dateDim.values.tolist()] # format data into a list of tuples before inserting to database
# dateDimList: data for date_dim table
dateDim.date = pd.to_datetime(dateDim.date) # convert data in date column to datatype date

factsStockDF = pd.merge(netflix_stocks, dateDim, left_on='Date', right_on='date', how='inner') # inner join dataframes netflix_stocks and dataDim on date
factsStockDF = factsStockDF.drop(['Date', 'date', 'year'], axis=1) # delete columns that are not needed
factsStockDF = factsStockDF.reindex(columns=['date_id','Open','High','Low','Close','Adj Close', 'Volume']) # re-arrange the order of columns
# factsStockDF['Date'] = factsStockDF['Date'].dt.strftime("%Y-%m-%d")
factsStockList = [tuple(l) for l in factsStockDF.values.tolist()] # format data into a list of tuples before inserting to database
# factsStockList: data for facts_stock_prices table
factsRating = pd.merge(netflix_titles, netflix_origionals, left_on='title', right_on='Title', how='left') # left join dataframes netflix_titles and netflix_originals on titles
factsRating.date_added = pd.to_datetime(factsRating.date_added) # convert data in date_added column to datatype date
factsRatingDF = pd.merge(factsRating, dateDim, left_on='date_added', right_on='date', how='left') # left join dataframes factsRating and dateDim on date_added/date
factsRatingDF = factsRatingDF[['show_id','title','Title','date_id','listed_in','rating']] # select only columns needed 
# factsRatingDF['date_id'] = factsRatingDF['date_id'].astype(str)
factsRatingDF = factsRatingDF.where((pd.notnull(factsRatingDF)), None) # convert empty values to "None" values
factsRatingList = [tuple(l) for l in factsRatingDF.values.tolist()] # format data into a list of tuples before inserting to database
#print(factsRatingList) # data for facts_IMDB_rating table
# create show_dim table and insert data into it
db = pymysql.connect(host="localhost", user="root", password="4321", database="mysql") # mysql connection credentials
cursor = db.cursor()

cursor.execute("DROP TABLE IF EXISTS show_dim;") # delete the table if it already exists

# create table with attributes based on the dimensional model
createTable1 = """CREATE TABLE show_dim(
                 show_id INT not null,
                 type VARCHAR(255) null,
                 title VARCHAR(255) not null,
                 director VARCHAR(255) null,
                 cast MEDIUMTEXT null,
                 country VARCHAR(255) null,
                 duration VARCHAR(255) null,
                 description MEDIUMTEXT null,
                 PRIMARY KEY (show_id, title));"""

cursor.execute(createTable1)

insertData1 = "INSERT INTO show_dim VALUES (%s,%s,%s,%s,%s,%s,%s,%s);" # insert data into table
cursor.executemany(insertData1, showDimList)
db.commit()

db.close() # close the connection to mysql
# create original_dim table and insert data into it
db = pymysql.connect(host="localhost", user="root", password="4321", database="mysql") # mysql connection credentials
cursor = db.cursor()

cursor.execute("DROP TABLE IF EXISTS original_dim;") # delete the table if it already exists

# create table with attributes based on the dimensional model
createTable2 = """CREATE TABLE original_dim(
                  original_title VARCHAR(255) not null,
                  seasons VARCHAR(255) null,
                  length VARCHAR (255) null,
                  regions VARCHAR (255) null,
                  status VARCHAR (255) null,
                  PRIMARY KEY (original_title));"""

cursor.execute(createTable2)

insertData2 = "INSERT INTO original_dim VALUES (%s,%s,%s,%s,%s);" # insert data into table
cursor.executemany(insertData2, netflix_origionalsList)
db.commit()

db.close() # close the connection to mysql
# create date_dim table and insert data into it
db = pymysql.connect(host="localhost", user="root", password="4321", database="mysql") # mysql connection credentials
cursor = db.cursor()

cursor.execute("DROP TABLE IF EXISTS date_dim;") # delete the table if it already exists

# create table with attributes based on the dimensional model
createTable3 = """CREATE TABLE date_dim(
                  date_id VARCHAR(50) not null,
                  date DATE null,
                  year INT null,
                  PRIMARY KEY (date_id));"""

cursor.execute(createTable3)

insertData3 = "INSERT INTO date_dim VALUES (%s,%s,%s);" # insert data into table
cursor.executemany(insertData3, dateDimList)
db.commit()

db.close() # close the connection to mysql
# create genre_dim table and insert data into it
db = pymysql.connect(host="localhost", user="root", password="4321", database="mysql") # mysql connection credentials
cursor = db.cursor()

cursor.execute("DROP TABLE IF EXISTS genre_dim;") # delete the table if it already exists

# create table with attributes based on the dimensional model
createTable4 = """CREATE TABLE genre_dim(
                  listed_in VARCHAR(255) not null,
                  PRIMARY KEY (listed_in));"""

cursor.execute(createTable4)

insertData4 = "INSERT INTO genre_dim VALUES (%s);" # insert data into table
val = [[item] for item in genreList]
cursor.executemany(insertData4, val)
db.commit()

db.close() # close the connection to mysql
# create facts_imdb_rating table and insert data into it
db = pymysql.connect(host="localhost", user="root", password="4321", database="mysql") # mysql connection credentials
cursor = db.cursor()

cursor.execute("DROP TABLE IF EXISTS facts_imdb_rating;") # delete the table if it already exists

# create table with attributes based on the dimensional model
createTable5 = """CREATE TABLE facts_imdb_rating(
                  show_id INT not null,
                  title VARCHAR(255) not null,
                  original_title VARCHAR(255) null,
                  date_id VARCHAR(50) null,
                  listed_in VARCHAR(255) null,
                  rating FLOAT null,
                  PRIMARY KEY (show_id),
                  FOREIGN KEY (show_id, title) REFERENCES show_dim(show_id, title),
                  FOREIGN KEY (original_title) REFERENCES original_dim(original_title),
                  FOREIGN KEY (date_id) REFERENCES date_dim(date_id),
                  FOREIGN KEY (listed_in) REFERENCES genre_dim(listed_in)
                  );"""

cursor.execute(createTable5)

insertData5 = "INSERT INTO facts_imdb_rating VALUES (%s,%s,%s,%s,%s,%s);" # insert data into table
cursor.executemany(insertData5, factsRatingList)
db.commit()

db.close() # close the connection to mysql
# create facts_stock_prices table and insert data into it
db = pymysql.connect(host="localhost", user="root", password="4321", database="mysql") # mysql connection credentials
cursor = db.cursor()

cursor.execute("DROP TABLE IF EXISTS facts_stock_prices;") # delete the table if it already exists

# create table with attributes based on the dimensional model
createTable6 = """CREATE TABLE facts_stock_prices(
                  date_id VARCHAR(50) not null,
                  open FLOAT null,
                  high FLOAT null,
                  low FLOAT null,
                  close FLOAT null,
                  adj_close FLOAT null,
                  volume FLOAT null,
                  PRIMARY KEY (date_id),
                  FOREIGN KEY (date_id) REFERENCES date_dim(date_id)
                  );"""

cursor.execute(createTable6)

insertData6 = "INSERT INTO facts_stock_prices VALUES (%s,%s,%s,%s,%s,%s,%s);" # insert data into table
cursor.executemany(insertData6, factsStockList)
db.commit()

db.close() # close the connection to mysql