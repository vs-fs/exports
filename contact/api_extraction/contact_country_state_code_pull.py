#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import base64
from simple_salesforce import Salesforce, SalesforceLogin, SFType
#from salesforce_api import Salesforce
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


# In[64]:


# Consumer Key
client_id = '3MVG9BJjUUIJZf1w9wByrIv..9QhrMmdX1m5..srp7GMgSKUm3zCpDk5nnOJeMwkhwoT5diK.8Dw5gYNUAXAI'

# Consumer Secret
client_secret = 'FF00C92A4C3AA32EA39CC06CEDD5826F9893A33B57DDD953EAB8F29ED17CA35F'


# In[65]:


# Callback URL
redirect_uri = 'http://localhost/'

# sfdc_user = your SFDC username
sfdc_user = 'test-epjcbbo27zdc@example.com'

# sfdc_pass = your SFDC password
sfdc_pass = ')2wZwptwzyjhp'


# In[66]:


auth_url = 'https://test.salesforce.com/services/oauth2/token'


# In[67]:


# POST request for access token
response = requests.post(auth_url, data = {
                    'client_id':client_id,
                    'client_secret':client_secret,
                    'grant_type':'password',
                    'username':sfdc_user,
                    'password':sfdc_pass
                    })


# In[68]:


# Retrieve token
json_res = response.json()
access_token = json_res['access_token']
auth = {'Authorization':'Bearer ' + access_token}


# In[69]:


# In some cases, instance_url may be different from your base URL, so it's best to extract it from response.json()
instance_url = json_res['instance_url']


# In[70]:


print(instance_url)


# In[71]:


# GET requests
url = instance_url + '/services/data/v56.0/sobjects/Contact/describe'
response = requests.get(url, headers=auth)
r = response.json()
print(r)


# In[72]:


data = json.loads(response.text)


# In[73]:


print(data)


# In[75]:


for field in data["fields"]:
    if field["name"] == "OtherStateCode":
        print("State code field: " + field["name"])
    if field["name"] == "OtherCountryCode":
        print("Country code field: " + field["name"])


# In[76]:


for field in data:
    print(field)


# In[77]:


for v in data.values():
    print(v)


# In[93]:


data["fields"][14]


# In[95]:


data["fields"][14]['picklistValues']


# In[96]:


cc = data["fields"][14]['picklistValues']


# In[97]:


cc_values = [c['value'] for c in cc]


# In[98]:


cc_df = pd.DataFrame(cc_values, columns=['country_code'])


# In[99]:


cc_df.head()


# In[100]:


pd.set_option('display.max_rows', None)


# In[101]:


print(cc_df)


# In[102]:


cc_df.to_sql(con=engine,name='contact_country_codes',if_exists='replace', index=False)


# In[103]:


data["fields"][13]


# In[104]:


data["fields"][13]['picklistValues']


# In[105]:


sc = data["fields"][13]['picklistValues']


# In[106]:


sc_values = [(s['value'], s['validFor']) for s in sc]


# In[107]:


sc_df = pd.DataFrame(sc_values, columns=['state_code', 'validFor'])


# In[108]:


print(sc_df)


# In[109]:


sc_df.to_sql(con=engine,name='contact_state_codes',if_exists='replace', index=False)


# In[ ]:




