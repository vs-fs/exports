#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import base64
from simple_salesforce import Salesforce, SalesforceLogin, SFType
import requests
import json
import pandas as pd
import sqlalchemy
import pymysql


# In[2]:


# Setting up environment variables

username = os.getenv('username')
password = os.getenv('password')
db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_name = os.getenv('db_name')


# In[3]:


engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@localhost:3600/{db_name}")


# In[4]:


# Consumer Key
client_id = '3MVG9BJjUUIJZf1w9wByrIv..9QhrMmdX1m5..srp7GMgSKUm3zCpDk5nnOJeMwkhwoT5diK.8Dw5gYNUAXAI'

# Consumer Secret
client_secret = 'FF00C92A4C3AA32EA39CC06CEDD5826F9893A33B57DDD953EAB8F29ED17CA35F'


# In[5]:


# Callback URL
redirect_uri = 'http://localhost/'

# sfdc_user = your SFDC username
sfdc_user = 'test-epjcbbo27zdc@example.com'

# sfdc_pass = your SFDC password
sfdc_pass = ')2wZwptwzyjhp'


# In[6]:


auth_url = 'https://test.salesforce.com/services/oauth2/token'


# In[7]:


# POST request for access token
response = requests.post(auth_url, data = {
                    'client_id':client_id,
                    'client_secret':client_secret,
                    'grant_type':'password',
                    'username':sfdc_user,
                    'password':sfdc_pass
                    })


# In[8]:


# Retrieve token
json_res = response.json()
access_token = json_res['access_token']
auth = {'Authorization':'Bearer ' + access_token}


# In[9]:


# In some cases, instance_url may be different from your base URL, so it's best to extract it from response.json()
instance_url = json_res['instance_url']


# In[10]:


print(instance_url)


# In[11]:


# GET requests
url = instance_url + '/services/data/v56.0/sobjects/Account/describe'
response = requests.get(url, headers=auth)
r = response.json()
# print(r)


# In[12]:


data = json.loads(response.text)


# In[14]:


for field in data["fields"]:
    if field["name"] == "BillingState":
        print("State code field: " + field["name"])
    if field["name"] == "BillingCountry":
        print("Country code field: " + field["name"])


# In[19]:


cc = data["fields"][12]['picklistValues']


# In[20]:


cc_values = [c['value'] for c in cc]


# In[21]:


cc_df = pd.DataFrame(cc_values, columns=['country_code'])


# In[23]:


pd.set_option('display.max_rows', None)


# In[25]:


cc_df.to_sql(con=engine,name='country_codes',if_exists='replace', index=True)


# In[28]:


sc = data["fields"][11]['picklistValues']


# In[29]:


sc_values = [(s['value'], s['validFor']) for s in sc]


# In[30]:


sc_df = pd.DataFrame(sc_values, columns=['state_code', 'validFor'])


# In[32]:


sc_df.to_sql(con=engine,name='state_codes',if_exists='replace', index=False)


# In[ ]:




