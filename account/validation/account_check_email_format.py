#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import os
import pandas as pd
import sqlalchemy
import pymysql
import mysql.connector


# In[2]:


# Setting up environment variables

db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[3]:


# Setting up connection to MySQL

engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")
cnx = mysql.connector.connect(user=db_user, password=db_pass,
                              host='localhost', port=3600, database=db_name)
cursor = cnx.cursor()


# In[4]:


# Define a regular expression pattern for email addresses
email_pattern = re.compile(r'[^@]+@[^@]+\.[^@]+')


# In[5]:


# Ingest the client_data and account tables as dataframes

df = pd.read_sql('Account', engine)


# In[6]:


# Check the "ORDERAPI__ACCOUNT_EMAIL__C" column
for index, row in df.iterrows():
    orderapi_email = str(row['ORDERAPI__ACCOUNT_EMAIL__C']) if not pd.isnull(row['ORDERAPI__ACCOUNT_EMAIL__C']) else ''
    if orderapi_email and not email_pattern.match(orderapi_email):
        error_message = "Invalid email format: " + orderapi_email
        cursor.execute("INSERT INTO error_messages (error_message) VALUES (%s)", (error_message,))

# Commit the changes to the database
cnx.commit()


# In[7]:


# Close the cursor and connection
cursor.close()
cnx.close()


# In[ ]:




