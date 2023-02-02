#!/usr/bin/env python
# coding: utf-8

# In[119]:


import re
import os
import pandas as pd
import sqlalchemy
import pymysql
import mysql.connector


# In[120]:


# Setting up environment variables

db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[121]:


# Setting up connection to MySQL

engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")
cnx = mysql.connector.connect(user=db_user, password=db_pass,
                              host='localhost', port=3600, database=db_name)
cursor = cnx.cursor()


# In[122]:


def validate_phone_number(number):
    if not pd.notnull(number) or number.strip() == '':
        return True
    pattern = re.compile(r'^\d{10}$|^\d{3}-\d{3}-\d{4}$')
    return pattern.match(number)


# In[123]:


def log_error(error_message, cursor, connection):
    add_error = ("INSERT INTO error_messages (error_message) VALUES (%s)")
    cursor.execute(add_error, (error_message,))
    connection.commit()


# In[124]:


# Ingest the client_data and account tables as dataframes

df = pd.read_sql('Account', engine)


# In[125]:


# Loop through each row in the DataFrame
for index, row in df.iterrows():
    fax = row['FAX']
    phone = row['PHONE']
    
    # Check if the fax number is valid
    if not validate_phone_number(fax):
        log_error(f'Invalid fax number: {fax}', cursor, cnx)
        
    # Check if the phone number is valid
    if not validate_phone_number(phone):
        log_error(f'Invalid phone number: {phone}', cursor, cnx)


# In[126]:


# Close the cursor and connection
cursor.close()
cnx.close()


# In[ ]:




