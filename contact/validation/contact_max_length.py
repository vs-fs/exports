#!/usr/bin/env python
# coding: utf-8

# In[65]:


import os
import pandas as pd
import sqlalchemy
import pymysql
import mysql.connector


# In[66]:


# Setting up environment variables

db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[67]:


# Setting up connection to MySQL

engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")
cnx = mysql.connector.connect(user=db_user, password=db_pass,
                              host='localhost', port=3600, database=db_name)
cursor = cnx.cursor()


# In[68]:


# Ingest the client_data and account tables as dataframes

df1 = pd.read_sql('Contact', engine)
df2 = pd.read_sql('contact_metadata', engine)


# In[69]:


df1 = df1.astype(str)
df2 = df2.astype(str)


# In[70]:


# Convert column names to lower case
df1.columns = [col.lower() for col in df1.columns]
df2['name'] = [col.lower() for col in df2['name']]


# In[71]:


for col in df1.columns:
    # Find the corresponding column in df2
    match = df2[df2['name'] == col]
    # Check if column name is found in df2
    if match.empty:
        cursor.execute("INSERT INTO error_messages (error_message) VALUES (%s)", (f"{col} not found",))
        cnx.commit()
        continue
    # Get the maximum length for the column
    max_length = int(match['length'].values[0])
    # if max_length = 0, ignore and continue
    if max_length == 0:
        continue
    # Check if any entries in the column exceed the maximum length
    invalid_rows = df1[df1[col].apply(lambda x: isinstance(x, str) and len(x) > max_length if not pd.isna(x) else False)]
    # Iterate over the rows with errors
    for index, row in invalid_rows.iterrows():
        # Insert error message into MySQL table
        cursor.execute("INSERT INTO error_messages (error_message) VALUES (%s)", (f"value {row[col]} in column {col} exceeds max length of {max_length}",))
    # Commit the errors to the database
    cnx.commit()


# In[72]:


# Close the cursor and the connection
cursor.close()
cnx.close()


# In[ ]:




