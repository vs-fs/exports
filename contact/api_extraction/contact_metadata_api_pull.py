#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin, SFType
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


class SalesforceMetadata:
    def __init__(self, username, password, security_token=None, domain=None):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.domain = domain 
        self.salesforce = None

    def connect_to_salesforce(self):
        try:
            self.session_id, self.instance = SalesforceLogin(username=self.username, password=self.password, security_token=self.security_token, domain=self.domain)
            self.salesforce = Salesforce(session_id=self.session_id, instance=self.instance)
            print(f'Connected to "{self.instance}"')
        except Exception as e:
            print(e)

    def salesforce_org_metadata_list(self):
        """
        return Salesforce org objects metafsys
        """
        if self.salesforce is None:
            print('Please connect to Salesforce first')
            return
        df = pd.DataFrame(self.salesforce.describe())
        df = df['sobjects'].apply(pd.Series)
        return df

    def object_metadata(self, object_api_name):
        if self.salesforce is None:
            print('Please connect to Salesforce first')
            return
        try:
            obj = SFType(object_api_name, session_id=self.session_id, sf_instance=self.instance)
            df = pd.DataFrame(obj.describe()['fields'])
            return df
        except Exception as e:
            print(e)
                
    def extract_picklist_values(self, object_metadata_df, field_api_name):
        picklist_values = []
        picklist_list_column = (object_metadata_df[object_metadata_df['name']==field_api_name]['picklistValues'].apply(pd.Series).T)
        if picklist_list_column.empty:
            print('Picklist is empty.')
            return
        for item in (object_metadata_df[object_metadata_df['name']==field_api_name]['picklistValues'].apply(pd.Series).T).iterrows():
            picklist_values.append(item[1].values[-1]['value'])
        return picklist_values


# In[5]:


domain = 'test'
security_token = None


# In[6]:


# construct sf_metadata object
sf_metadata = SalesforceMetadata(username, password, security_token, domain)


# In[7]:


# connect to Salesforce
sf_metadata.connect_to_salesforce()


# In[8]:


# return organization metadata (this will return overall detail of every single Saleforce object in an org)
df_org_metadata = sf_metadata.salesforce_org_metadata_list()
df_org_metadata.to_csv('org metadata sample.csv', index=False)
# print(df_org_metadata)


# In[9]:


# to return just an object's metadata as a dataframe
df_account = sf_metadata.object_metadata('account')
df_opportunity = sf_metadata.object_metadata('opportunity')


# In[10]:


# push data to MySQL db
object_list = ['contact']
for object_api_name in object_list:
    df = sf_metadata.object_metadata(object_api_name)
    df["plv"] = df["picklistValues"].astype(str)
    df["rt"] = df["referenceTo"].astype(str)
    df = df.drop(['picklistValues', 'referenceTo'], axis=1)
    if df is None:
        print(f'Object {object_api_name} is not found.')
    else:
        df.to_sql(con=engine,name=f'{object_api_name}_metadata',if_exists='replace', index=True)


# In[ ]:




