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


def validate_phone_number(number):
    if not pd.notnull(number) or number.strip() == '':
        return True
    pattern = re.compile(r'^\d{10}$|^\d{3}-\d{3}-\d{4}$')
    return pattern.match(number)


# In[5]:


def log_error(error_message, cursor, connection):
    add_error = ("INSERT INTO error_messages (error_message) VALUES (%s)")
    cursor.execute(add_error, (error_message,))
    connection.commit()


# In[6]:


# Ingest the client_data and account tables as dataframes

df = pd.read_sql('Contact', engine)


# In[7]:


# Loop through each row in the DataFrame
for index, row in df.iterrows():
    assistant = row['ASSISTANTPHONE']
    phone = row['PHONE']
    mobile = row['MOBILEPHONE']
    preferred = row['ORDERAPI__PREFERRED_PHONE__C']
    
    # Check if the fax number is valid
    if not validate_phone_number(assistant):
        log_error(f'Invalid Assistant phone number: {assistant}', cursor, cnx)
        
    # Check if the phone number is valid
    if not validate_phone_number(phone):
        log_error(f'Invalid phone number: {phone}', cursor, cnx)
    
    # Check if the phone number is valid
    if not validate_phone_number(mobile):
        log_error(f'Invalid mobile number: {mobile}', cursor, cnx)
        
    # Check if the phone number is valid
    if not validate_phone_number(preferred):
        log_error(f'Invalid preferred phone number: {preferred}', cursor, cnx)


# In[8]:


# Close the cursor and connection
cursor.close()
cnx.close()


# In[ ]:




