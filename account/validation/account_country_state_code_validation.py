#!/usr/bin/env python
# coding: utf-8

# In[12]:


import os
import pandas as pd
import base64
import numpy as np
import pymysql
import sqlalchemy
import mysql.connector


# In[13]:


# Setting up environment variables

db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[14]:


# Setting up connection to MySQL

engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")
cnx = mysql.connector.connect(user=db_user, password=db_pass,
                              host='localhost', port=3600, database=db_name)
cursor = cnx.cursor()


# In[15]:


# Ingest the client_data and account tables as dataframes

account = pd.read_sql('Account', engine)
df2 = pd.read_sql('account_metadata', engine)
cc_df = pd.read_sql('country_codes', engine)
sc_df = pd.read_sql('state_codes', engine)


# In[16]:


account.head()


# In[17]:


# def validate_account(account, cc_df, sc_df):
#     # Connect to MySQL database
#     # connection = pymysql.connect(host='localhost',
#     #                              user='db_user',
#     #                              password='db_pass',
#     #                              db='db_name',
#     #                              charset='utf8mb4',
#     #                              cursorclass=pymysql.cursors.DictCursor)

#     df_new = pd.DataFrame()
#     # Step 1: Iterate through 'account' and check if country_code is valid
#     for i, row in account.iterrows():
#         country_code = row['BILLINGCOUNTRYCODE']
#         state_code = row['BILLINGSTATECODE']
#         if country_code in cc_df['country_code'].values:
#             df_new = df_new.append(row)
#         else:
#             # Send error message to 'error_messages' MySQL table
#             # with connection.cursor() as cursor:
#             #     sql = "INSERT INTO error_messages (error_text) VALUES (%s)"
#             #     cursor.execute(sql, (f'Invalid country code: {country_code}'))
#             #     connection.commit()
#             print(country_code)
#             print("Error Country code")
    
#     # Step 2: Decode Base64 and convert to binary
#     sc_df['validFor'] = sc_df['validFor'].apply(lambda x: bin(int.from_bytes(base64.b64decode(x), 'big'))[2:])
    
#     # Step 3: Calculate position of first 1 in binary string
#     sc_df['validFor'] = sc_df['validFor'].apply(lambda x: x.find('1') - 1)
    
#     # Step 4: Iterate through 'df_new' and check if state_code is valid
#     for i, row in df_new.iterrows():
#         country_code = row['BILLINGCOUNTRYCODE']
#         state_code = row['BILLINGSTATECODE']
#         # Find corresponding 'validFor' value in 'sc_df'
#         valid_states = sc_df.loc[sc_df['validFor'].str[int(cc_df.loc[cc_df['country_code'] == country_code].index[0])] == '1']
        
#         print(valid_states)
#         # check if state_code is valid
#         if not state_code in valid_states['state_code'].values:
#             print('State error')
#             # Send error message to 'error_messages' MySQL table
#             # with connection.cursor() as cursor:
#             #     sql = "INSERT INTO error_messages (error_text) VALUES (%s)"
#             #     cursor.execute(sql, (f'Invalid state code: {state_code}'))
#             #     connection.commit()

#     # finally:
#     #     connection.close()
#     # return df_new


# In[22]:


def count_zeros_before_first_one(string):
    string = string.replace(" ", "")
    first_one_index = string.find("1")
    return 0 if first_one_index == -1 else first_one_index

def validate_account(account, cc_df, sc_df):
    # Connect to MySQL database
    # connection = pymysql.connect(host='localhost',
    #                              user='db_user',
    #                              password='db_pass',
    #                              db='db_name',
    #                              charset='utf8mb4',
    #                              cursorclass=pymysql.cursors.DictCursor)

  df_new = pd.DataFrame()
# Step 1: Iterate through 'account' and check if country_code is valid
  for i, row in account.iterrows():
      country_code = row['BILLINGCOUNTRYCODE']
      state_code = row['BILLINGSTATECODE']
      if country_code in cc_df['country_code'].values:
          df_new = df_new.append(row)
      else:
          # Send error message to 'error_messages' MySQL table
          # with connection.cursor() as cursor:
          #     sql = "INSERT INTO error_messages (error_text) VALUES (%s)"
          #     cursor.execute(sql, (f'Invalid country code: {country_code}'))
          #     connection.commit()
          print(f"Country code {country_code} is not valid")
  # Step 2: Decode Base64 and convert to binary
  # Convert the column to binary
  sc_df['validFor_binary'] = sc_df['validFor'].apply(lambda x: " ".join(format(b, '08b') for b in base64.b64decode(x)))

  sc_df['one_index'] = sc_df['validFor_binary'].apply(count_zeros_before_first_one)


  # Step 3: Iterate through 'df_new' and check if state_code is valid
  for i, row in df_new.iterrows():
    country_code = row['BILLINGCOUNTRYCODE']
    state_code = row['BILLINGSTATECODE']

    # Find corresponding 'validFor' value in 'sc_df'
    # Get the first_one value for the state code
    state_code_first_one = sc_df.loc[sc_df["state_code"] == state_code, "one_index"]
    # Get the index value for the country code in cc_df
    cc_index = cc_df.loc[cc_df['country_code'] == country_code, "index"].item()
    # check if state_code is valid
    if pd.isna(state_code):
      print("State code is NaN, skipping...")
      continue
    
    if state_code not in sc_df['state_code'].tolist():
      print('Invalid state code')
      continue
      
    # Check if the state code is valid by comparing its first '1' position with the country code's index
    if state_code_first_one.iat[0] == cc_index:
      print("Valid state code for country:", country_code, " state code: ", state_code, state_code_first_one.iat[0])
    else:
      print("Invalid state code for country:", country_code, " state code: ", state_code, state_code_first_one.iat[0])
      # Send error message to 'error_messages' MySQL table
      # with connection.cursor() as cursor:
      #     sql = "INSERT INTO error_messages (error_text) VALUES (%s)"
      #     cursor.execute(sql, (f'Invalid state code: {state_code}'))
      #     connection.commit()



    # finally:
    #     connection.close()
    # return df_new


# In[23]:


validate_account(account, cc_df, sc_df)


# In[19]:


sc_df.head()


# In[59]:


df2 = pd.read_csv('Account.csv')
cc_df = pd.read_csv('country_codes.csv')
sc_df = pd.read_csv('state_codes.csv')
df = validate_account(df2, cc_df, sc_df)

