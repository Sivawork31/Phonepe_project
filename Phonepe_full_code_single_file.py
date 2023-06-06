#!/usr/bin/env python
# coding: utf-8

# In[128]:


import streamlit  as st
import json 
import os
import pandas as pd
import numpy as np
import mysql.connector as mc
connection = mc.connect(user= 'root',password='ss3112',host = 'localhost')
cursor = connection.cursor()
cursor.execute('CREATE DATABASE phonepe_pulse')
cursor.execute('USE phonepe_pulse')


# In[129]:


# This is to direct the path to get the data as states

path = "pulse/data/aggregated/transaction/country/india/state/"
user_state_list = os.listdir(path)
#Agg_state_list
# Agg_state_list--> to get the list of states in India

#<------------------------------------------------------------------------------------------------------------------->#

# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'Transaction_type': [],
       'Transaction_count': [], 'Transaction_amount': []}
for i in user_state_list:
    p_i = path+i+"/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i+j+"/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transaction_type'].append(Name)
                    clm['Transaction_count'].append(count)
                    clm['Transaction_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
            except:
                pass
# Succesfully created a dataframe
Agg_Transac = pd.DataFrame(clm)
#Agg_Trans.to_csv(r 'C:\Users\rsiva\Desktop\project phonepe\prjt_csv\Agg_trans.csv')

Agg_Transac.to_csv('phonepe_csv/Agg_Transac.csv',index = False)
Agg_Transac_csv = pd.read_csv('phonepe_csv/Agg_Transac.csv')
#Agg_Trans
Agg_Transac


# In[130]:


cursor.execute("create table aggregate_transaction(state varchar(45), year int, Quater int, tansaction_type varchar(45), transaction_count int, transaction_amount float)")
for record in Agg_Transac.values:
    cursor.execute("insert into aggregate_transaction(state, year, Quater, tansaction_type, transaction_count, transaction_amount) values{}".format(tuple(record)))
    connection.commit()
print('Data inserted into Table-1 successfully')
    

#**************************************** ------  1 st finished  ------************************************************
    
    


# In[ ]:





# In[131]:


#                         2.  This is to direct the path to get the data as users per states:

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


path = "pulse/data/aggregated/user/country/india/state/"
user_state_list = os.listdir(path)


#---------------------------------------------------------------------------------------------------------------------


# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'Brand': [],
    'Brand_count': [], 'Brand_percentage': []}
for i in user_state_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["usersByDevice"]:
                    
                    brand = z['brand']

                    brand_count = z['count']
                    brand_percentage = z["percentage"]
                    clm['Brand'].append(brand)
                    clm['Brand_count'].append(brand_count)
                    clm['Brand_percentage'].append(brand_percentage)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
            except:
                pass 
userby_device = pd.DataFrame(clm)
# Data frame is created successfully and now we can save df in csv format
userby_device.to_csv("phonepe_csv/userby_device.csv")
userby_device


# In[132]:


cursor.execute("create table userby_device(state varchar(45),year int,Quarter int,users_brand varchar(45),users_count int,user_percentage float)")
for record in userby_device.values:
    cursor.execute("insert into userby_device(state, year, Quarter, users_brand, users_count, user_percentage) values{}".format(tuple(record)))
    connection.commit()
print('Data inserted into Table-2 successfully')
    

#**************************************** ------  2 nd finished  ------************************************************



# In[ ]:





# In[133]:


#                                  3.   This is for map_transaction:

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


path = "pulse/data/map/transaction/hover/country/india/state/"
state_list = os.listdir(path)
#Agg_state_list
# Agg_state_list--> to get the list of states in India


#-----------------------------------------------------------------------------------------------------------------------

# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'District': [],
    'Transaction_count': [], 'Transaction_amount': []}
for i in state_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["hoverDataList"]:
                    district = z['name']
                    transaction_count = z['metric'][0]['count']
                    transaction_amount = z['metric'][0]['amount']
                    clm['District'].append(district)
                    clm['Transaction_count'].append(transaction_count)
                    clm['Transaction_amount'].append(transaction_amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))

            except:
                pass   
                
map_transaction = pd.DataFrame(clm)
# Succesfully created a dataframe
# map_transaction.to_csv("phonepe_csv/map_transaction.csv")
map_transaction



# In[134]:


cursor.execute("create table map_transaction(state varchar(45),year int, quarter int,district_name varchar(45),transaction_count int, transaction_amount float)")

for record in map_transaction.values:
    cursor.execute("insert into map_transaction(state,year,quarter,district_name,transaction_count,transaction_amount)values{}".format(tuple(record)))
    connection.commit()
print('Data inserted into Table-3 successfully')
    
#**************************************** ------  3 rd finished  ------************************************************



# In[ ]:





# In[135]:


#                            4.  This is registered users in India :

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


path = "pulse/data/map/user/hover/country/india/state/"
state_list = os.listdir(path)
#Agg_state_list
# Agg_state_list--> to get the list of states in India

#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'District': [],
    'Registered_user': [], 'App_opening': []}
for i in state_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["hoverData"]:
                    district = z
                    registered_user =  D['data']["hoverData"][z]["registeredUsers"]
                    app_opening = D['data']["hoverData"][z]["appOpens"]
                    clm['District'].append(district)
                    clm['Registered_user'].append(registered_user)
                    clm['App_opening'].append(app_opening)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))

            except:
                pass       
                 
registered_users = pd.DataFrame(clm)
# Succesfully created a dataframe
#registered_users.to_csv("phonepe_csv/district_registering.csv")

registered_users


# In[136]:


cursor.execute("create table registered_users(state varchar(45),year int,quarter int,district_name varchar(45), registered_users int, app_opens int)")
for record in district_registering.values:
    cursor.execute("insert into registered_users(state, year, quarter, district_name, registered_users, app_opens)values{}".format(tuple(record)))
    connection.commit()
print('Data inserted into Table-4 successfully')
    

#**************************************** ------  4 th finished  ------************************************************



# In[ ]:





# In[137]:


#                       5.  This is extract the data of top transactions made in all states:

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


path = 'pulse/data/top/transaction/country/india/state/'
state_list = os.listdir(path)
clm={'State':[],'Year':[],'Quarter':[],'district_name':[] ,'total_count':[],'total_amount':[]}

#--------------------------------------------------------------------------------------------------------------------------


for i in state_list:
    p_i= path + i + '/'
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i + j + '/'
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j + k 
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['districts']:
                    district_name = z['entityName']
                    count= z['metric']['count']
                    amount = z['metric']['amount']
                   
                    clm['district_name'].append(district_name)
                    clm['total_count'].append(count)
                    clm['total_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip(".json")))

top_transac = pd.DataFrame(clm)
top_transac


# In[138]:


cursor.execute("create table top_transaction(state varchar(45),year int,quarter int,district_name varchar(45),transaction_count int,transaction_amount float)")
for record in top_transac.values:
    cursor.execute("insert into top_transaction(state,year,quarter,district_name,transaction_count,transaction_amount)values{}".format(tuple(record)))
    connection.commit()
print('Data inserted into Table-5 successfully')
    

#**************************************** ------  5 th finished  ------************************************************



# In[ ]:





# In[141]:


#                           6. This is to find the top users list in india:

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


path = 'pulse/data/top/user/country/india/state/'
state_list = os.listdir(path)
clm={'State':[],'Year':[],'Quarter':[],'district_name':[] ,'total_users':[]}


#---------------------------------------------------------------------------------------------------------------------------

for i in state_list:
    p_i= path + i + '/'
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i + j + '/'
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j + k 
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['districts']:
                    users = z['registeredUsers']
                    district_name = z['name']
                    clm['district_name'].append(district_name)
                    clm['total_users'].append(users)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip(".json")))

top_users = pd.DataFrame(clm)
top_users


# In[142]:


cursor.execute("create table top_users(state varchar(45),year int,quarter int,district_name varchar(45),registered_users int)")             
for record in top_users.values:
    cursor.execute("insert into top_users(state, year, quarter, district_name, registered_users) values{}".format(tuple(record)))
    connection.commit()
print('Data inserted into Table-6 successfully')
    

#**************************************** ------  6 th finished  ------************************************************



# In[ ]:


#                                  Streamlit app creation 


# In[ ]:


#webpage title

   st.set_page_config(page_title="PhonPe_Pulse",layout="wide")
st.title("Data science Project")
#st.header(":violet[_PhonePe-Pulse_]")
new_title = '<p style="font-family:Courier; color:purple; font-size: 40px;">PhoePe-Pulse</p>'
st.markdown(new_title, unsafe_allow_html=True) 
left_column,right_column=st.columns(2)  

with left_column:
   option=st.selectbox(label="**Transactions/Users**",options=("Transaction","Users"))
   if option=='Transaction':
       transaction_data=st.selectbox(label="**Name Of Transaction**",options=("aggregate_transaction","map_transaction","top_transaction"))
       if transaction_data=='aggregate_transaction':
           type_transaction=st.selectbox(label="**Transaction Type**",options=("Recharge & bill payments","Peer-to-peer payments","Merchant payments","Financial Services","Others"))
           Quater=st.selectbox(label="**Quater**",options=("1","2","3","4"))
           year=st.selectbox("**Year**",range(2018,2023))
           query2="select state,year,Quater,tansaction_type ,sum(transaction_amount) from {} group by state,year,Quater,tansaction_type having year={} and Quater={} and tansaction_type='{}' ".format(transaction_data,year,Quater,type_transaction)
           df=pd.read_sql_query(query2, connection)
           with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
               indian_states=json.load(response)
           list_states=[]
           for i in range(len(indian_states['features'])):
               state=indian_states['features'][i]['properties']['ST_NM']
               list_states.append(state)
           st_name=sorted(list_states)
           df['st_name']=st_name
           query="select sum(transaction_amount) from aggregate_transaction"
           total_trans=pd.read_sql_query(query, connection)
           with right_column:
               st.subheader("Total Transaction value")
               st.markdown(total_trans.values)
               st.subheader("{} value".format(type_transaction))
               st.markdown(df['sum(transaction_amount)'].sum())
               st.subheader("Average {} value".format(type_transaction))
               st.markdown(df['sum(transaction_amount)'].mean())
               data =px.choropleth(df,locations='st_name',
                                   title="Total {} of {}".format(transaction_data,type_transaction),
                                   geojson=indian_states,
                                   featureidkey='properties.ST_NM',
                                   color='sum(transaction_amount)',
                                   color_continuous_scale='Reds',
                                   scope='asia',
                                   projection="mercator",
                                   hover_data=['year','Quater']
                       
                                   )
               data.update_geos(fitbounds='locations', visible=False)
               st.plotly_chart(data,use_container_width=True)
       else:
           Quater=st.selectbox(label="**Quater**",options=("1","2","3","4"))
           year=st.selectbox("**Year**",range(2018,2023))
           query2="select state,year,Quater,sum(transaction_amount) from {} group by state,year,Quater having year={} and Quater={}".format(transaction_data,year,Quater)
           df=pd.read_sql_query(query2, connection)
           with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
               indian_states=json.load(response)
           list_states=[]
           for i in range(len(indian_states['features'])):
               state=indian_states['features'][i]['properties']['ST_NM']
               list_states.append(state)
           st_name=sorted(list_states)
           df['st_name']=st_name
           with right_column:
               st.subheader("Total transaction value")
               st.markdown(df['sum(transaction_amount)'].sum())  
               st.subheader("Average transaction value")
               st.markdown(df['sum(transaction_amount)'].mean())
               data =px.choropleth(df,locations='st_name',
                                   title="Total {}".format(transaction_data),
                                   geojson=indian_states,
                                   featureidkey='properties.ST_NM',
                                   color='sum(transaction_amount)',
                                   color_continuous_scale='Reds',
                                   scope='asia',
                                   projection="mercator",
                                   hover_data=['year','Quater']
                                  
                
                                   )
               data.update_geos(fitbounds='locations', visible=False)
               st.plotly_chart(data,use_container_width=True)
      
   elif option=='Users':
       users_data=st.selectbox(label="**Users Type**",options=("userby_device","registered_users","top_users"))
       if users_data=="userby_device":
           brand=st.selectbox(label="**Brand**",options=("Xiaomi","Samsung","Vivo","Oppo","OnePlus","Realme","Apple","Motorola","Lenovo","Huawei","Others"))
           Quater=st.selectbox(label="**Quater**",options=("1","2","3","4"))
           year=st.selectbox("**Year**",range(2018,2023))
           query2="select state,year,Quater,sum(users_count) from userby_device group by state,year,Quater having year={} and Quater={}".format(year,Quater)
           brand_query="select brand,sum(users_count) from userby_device group by brand"
           df=pd.read_sql_query(query2, connection)
           with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
               indian_states=json.load(response)
           list_states=[]
           for i in range(len(indian_states['features'])):
               state=indian_states['features'][i]['properties']['ST_NM']
               list_states.append(state)
           st_name=sorted(list_states)
           df['st_name']=st_name
           with right_column:
               st.subheader("{} Users".format(brand))
               st.markdown(df['sum(users_count)'].sum())
               
               st.subheader("Total Registered Users")
               st.markdown(df['sum(users_count)'].sum())
               data =px.choropleth(df,locations='st_name',
                                   title="{}".format(users_data),
                                   geojson=indian_states,
                                   featureidkey='properties.ST_NM',
                                   color='sum(users_count)',
                                   color_continuous_scale='Reds',
                                   scope='asia',
                                   projection="mercator",
                                   hover_data=['year','Quater']
                                  
                
                                   )
               data.update_geos(fitbounds='locations', visible=False)
               st.plotly_chart(data,use_container_width=True)
       else:
           Quater=st.selectbox(label="**Quater**",options=("1","2","3","4"))
           year=st.selectbox("**Year**",range(2018,2023))
           query2="select state,year,Quater,sum(registered_users) from {} group by state,year,Quater having year={} and Quater={}".format(users_data,year,Quater)
           df=pd.read_sql_query(query2, connection)
           with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson')as response:
               indian_states=json.load(response)
           list_states=[]
           for i in range(len(indian_states['features'])):
               state=indian_states['features'][i]['properties']['ST_NM']
               list_states.append(state)
           st_name=sorted(list_states)
           df['st_name']=st_name
           with right_column:
               st.subheader("Total Registered Users")
               st.markdown(df['sum(registered_users)'].sum())
               data =px.choropleth(df,locations='st_name',
                                   title="{}".format(users_data),
                                   geojson=indian_states,
                                   featureidkey='properties.ST_NM',
                                   color='sum(registered_users)',
                                   color_continuous_scale='Reds',
                                   scope='asia',
                                   projection="mercator",
                                   hover_data=['year','Quater']
                                  
                
                                   )
               data.update_geos(fitbounds='locations', visible=False)
               st.plotly_chart(data,use_container_width=True)
               

