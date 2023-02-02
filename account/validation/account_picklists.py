#!/usr/bin/env python
# coding: utf-8

# In[46]:


import os
import pandas as pd
import sqlalchemy
import pymysql
import mysql.connector


# In[47]:


# Setting up environment variables

db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[48]:


# Setting up connection to MySQL

engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")
conn = mysql.connector.connect(user=db_user, password=db_pass,
                              host='localhost', port=3600, database=db_name)
cursor = conn.cursor()


# In[49]:


# Ingest the client_data and account tables as dataframes

df1 = pd.read_sql('Account', engine)
df2 = pd.read_sql('account_metadata', engine)


# In[50]:


# 1. Convert all column names of df1 to lower case
df1.columns = [col.lower() for col in df1.columns]

# 2. Convert all contents of "name" column in df2 to lower case
df2["name"] = df2["name"].str.lower()

# 3. Convert all contents of 'plv' column in df2 to lower case
#df2['plv'] = df2['plv'].apply(lambda x: [dict(i, **{k.lower(): v.lower() for k, v in i.items()}) for i in x])
df2['plv'] = df2['plv'].apply(lambda x: [{k.lower(): v.lower() for k, v in d.items()} if type(d) == dict else d for d in x] if type(x) == list else x)


# In[51]:


# 4. Keep only "name" and "plv" columns in df2
df2 = df2[["name", "plv"]]


# In[52]:


# 5. Remove rows in df2 with "[]" in "plv" column
df2 = df2[df2.plv != '[]']


# In[55]:


# 6. Remove columns in df1 that don't match with values in "name" column in df2
cols_to_remove = set(df1.columns) - set(df2["name"])
df1.drop(cols_to_remove, axis=1, inplace=True)


# In[57]:


display(df2)


# In[56]:


display(df1)


# In[59]:


# 7. Check values in df1 columns against corresponding "plv" values in df2
error_msgs = []
for col in df1.columns:
    if col not in df2["name"].values:
        df1 = df1.drop(columns=[col])
        continue

    plv = df2.loc[df2["name"] == col, "plv"].iloc[0]
    for val in df1[col].unique():
        if str(val) not in plv:
            error_msgs.append(f"Value '{val}' in column '{col}' not found in its corresponding picklist")


# In[60]:


# 8. If no match is found, send error message to "error_messages" table in MySQL
if error_msgs:
    cursor = conn.cursor()
    for msg in error_msgs:
        sql = "INSERT INTO error_messages (error_message) VALUES (%s)"
        val = (msg,)
        cursor.execute(sql, val)
    conn.commit()
    cursor.close()


# In[61]:


# Close MySQL connection
conn.close()


# In[ ]:




