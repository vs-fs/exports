#!/usr/bin/env python
# coding: utf-8

# In[35]:


import os
import pandas as pd
import sqlalchemy
import pymysql
import mysql.connector


# In[36]:


# Setting up environment variables

db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[37]:


# Setting up connection to MySQL

engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")
cnx = mysql.connector.connect(user=db_user, password=db_pass,
                              host='localhost', port=3600, database=db_name)
cursor = cnx.cursor()


# In[38]:


# Ingest the client_data and account tables as dataframes

df1 = pd.read_sql('Contact', engine)
df2 = pd.read_sql('contact_metadata', engine)


# In[39]:


# 1. each column name in df1 should be converted to lower case
df1.rename(columns=lambda x: x.lower(), inplace=True)

# 2. the entirety of the "name" column in df2 should be converted to lower case
df2["name"] = df2["name"].str.lower()


# In[40]:


pd.set_option('display.max_rows', None)


# In[41]:


# 3. the entirety of the "nillable" column in df2 should be converted to boolean, if possible
df2["nillable"] = df2["nillable"].map({'False': False, 'True': True})
df2["nillable"] = df2["nillable"].astype("bool")


# In[42]:


# 4. if the "nillable" column can be converted to boolean, then it should be checked to see if all entries are True or if all entries are False. If so, a print statement with an error should be produced
nillable_col = df2["nillable"]
if (nillable_col.all() or (~nillable_col).all()):
    print("ERROR: All values in the 'nillable' column are either True or False")


# In[43]:


# 5. df2 should be transformed so that only the "name" and "nillable" columns remain
df2 = df2[["name", "nillable"]]


# In[44]:


# 6. df2 should be transformed so that all of the rows in the "nillable" column with a value of True are removed
df2 = df2[df2["nillable"] == False]


# In[45]:


# 7. each df1 column should be compared with each row in the "name" column to find its match. if a match is not found, then that df1 column should be removed from the df1 dataframe
cols_to_keep = df2["name"].tolist()
df1 = df1[cols_to_keep]


# In[47]:


# 8. all of the remaining columns in df1 should be scanned for NULL, NaN, empty or blank values
for col in df1.columns:
    mask = df1[col].isnull() | df1[col].eq("")
    if mask.any():
        error_message = f"ERROR: Column '{col}' contains NULL, NaN, empty, or blank values"
        # 9. if an error is found, then it should be sent to a MySQL table called "error_messages"
        cnx = mysql.connector.connect(
            host='localhost',
            port=3600,
            user=db_user,
            password=db_pass,
            database=db_name
        )
        cursor = cnx.cursor()
        query = "INSERT INTO error_messages (error_message) VALUES (%s)"
        cursor.execute(query, (error_message,))
        cnx.commit()
        cursor.close()
        cnx.close()


# In[ ]:




