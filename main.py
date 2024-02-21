import datetime, time
import streamlit as st
import pandas as pd
import numpy as np
import datatest as dt
import json
import requests
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
from uuid import uuid4
from PIL import Image

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials, project = "sirclo-prod")


def upload_bq(df):
    target_table = 'marketplace.rrp_promo'
    project_id = "sirclo-prod"
    df.to_gbq(target_table, project_id=project_id, if_exists='append', progress_bar=True, credentials=credentials)

def error_log(e,official_store):
    errors = pd.DataFrame(columns = ['official_store','uploader','error_message','created_at'])
    errors['error_message'] = e
    errors['official_store']=official_store
    errors['created_at']=pd.to_datetime(datetime.datetime.utcnow().strftime('%Y-%m-%d %X'))
    
def findtext(column,e):
    s = str(e)
    s = s.split("'")[1]
    filter= df[column]==s
    df2 = df.where(filter, inplace=False).dropna()
    return df2

#Function to determine point seperator
def get_ps(df,i):
    ps = "."
    ts = ","
    if df.loc[i].find('.')!=-1:
        (ps,ts) = (',','.') if len(df.loc[i].split('.')[1])>=3 else ('.',',')
    elif df.loc[i].find(',')!=-1 :
        (ps,ts) = ('.',',') if len(df.loc[i].split(',')[1])>=3 else (',','.')
    else:
        ps = "."
        ts = ","
    return df.loc[i].replace(ps,'koma').replace(ts,',').replace(',','').replace('koma','.')

def numerical_validation(df,column):
    try :
        if df[column].dtypes == 'object':
            df[column]=df[column].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
            for i in range (len(df)):
                df.loc[i,column] = get_ps(df[column],i)
            df[column] = df[column].astype(float).abs()
        else :
            df[column] = df[column].astype(float).abs()
    except ValueError as e:
        #error_log(e,official_store)
        st.error("Can't convert %s column to the correct format"%(column))
        expander = st.expander("Show details")
        df2 = findtext(column,e)
        expander.error(e)
        expander.dataframe(df2.style.set_properties(**{'background-color': 'red'},subset=[column]))
        exit()

header_footer = """ 
<style>
footer{
    visibility:visible;
}
footer:after{
    content:'Developed by data-intelligence@sirclo.com';
    display:block;
    position:relative;
    color:#3498DB;
}
<\style>
"""

im = Image.open('image/logo.png')
st.set_page_config(page_title="RRP, RBP, and Promo Plan",page_icon = im)
st.markdown(header_footer,unsafe_allow_html=True)
st.title("RRP, RBP, and Promo Plan")

string_to_replace_dict = {"rp.":"","rp":""}
column_dict = {'principal', 'brand', 'product_code', 'product_description',
    'product_type', 'marketplace', 'official_store', 'start_date',
    'end_date', 'activity','promo_campaign_name', 'rrp_incl_vat', 'promo_discount_percent',
    'promo_discount_amount', 'rrp_promo_incl_vat', 'rbp_incl_vat'}
product_type = ['Assembly','Single','Gimmick']
numerical_column = ['rrp_incl_vat', 'promo_discount_percent',
    'promo_discount_amount', 'rrp_promo_incl_vat', 'rbp_incl_vat']
varchar_column = ['principal','brand','product_code','product_description','product_type','marketplace',
    'official_store','activity']

SHEET_ID = '1ipc6iMh9adYIFcqzg_wnVt3ftzAUjCng7Z6fP5z6Ib4'
SHEET_NAME = 'os'
url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
df = pd.read_csv(url)
a = ",".join(df['official_store'])
official_store = st.selectbox('Please choose the official Store',a.split(','))

os_list = df['official_store'].tolist()
principal_list = df['principal'].dropna().tolist()

try:
    1=0
except:
    st.error('Please access the new platform in https://vision.sirclo.net/commercial')

# uploaded_file = st.file_uploader(label='Upload Input Files',label_visibility='collapsed')
# if uploaded_file is None :
#     st.error("Upload a .csv file first: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
#     exit()
# try:
#     df = pd.read_csv(uploaded_file)
#     dt.validate(df.columns,column_dict)
# except ValueError as e:
#     #error_log(e,official_store)
#     st.error("The file should be in .csv format, use this template: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
# except dt.ValidationError as e:
#     #error_log(e,official_store)
#     st.error("Invalid schema, please use this schema/template: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
#     headers = {
#     "selector": "th:not(.index_name)",
#     "props": "background-color: red;"
#     }
#     expander = st.expander("Show details")
#     expander.dataframe(df.style.set_table_styles([headers]).set_properties(**{'background-color': 'red'}))
#     exit()
# for i in varchar_column:
#     df[i] = df[i].str.strip()
# df['activity'].replace('Non Promo','Non-Promo',inplace=True)
# df['product_type'].replace('Gift','Gimmick',inplace=True)
# for i in range(len(df)):
#     if df.loc[i,'activity'] == 'Non-Promo':
#         df.loc[i,'promo_campaign_name'] = 'Non-Promo'
#     else :
#         df.loc[i,'promo_campaign_name'] = df.loc[i,'promo_campaign_name']
# df = df.dropna(axis = 0, how = 'all').reset_index(drop=True)
# df['promo_discount_percent'].fillna(0,inplace=True)
# df['promo_discount_amount'].fillna(0,inplace=True)
# df['rrp_promo_incl_vat'].fillna(0,inplace=True)
# if df.loc[:].isnull().values.any()==True:
#     total = len(df)
#     invalid_rows = [index for index, row in df.loc[:].iterrows() if row.isnull().any()]
#     invld = len(invalid_rows)
#     validation = ('Invalid Row '+str(invld)+'/'+str(total))
#     empty = df.columns[df.isna().any()].tolist()
#     e = "%s field cannot be empty"%(empty)
#     #error_log(e,official_store)
#     st.error("%s field cannot be empty"%(empty))
#     expander = st.expander("Show details")
#     expander.write(validation)
#     expander.dataframe(df.loc[invalid_rows].style.highlight_null(color='red'))
#     exit()
# try:
#     df['start_date']=pd.to_datetime(df['start_date'],format="%Y-%m-%d")
#     df['end_date']=pd.to_datetime(df['end_date'],format="%Y-%m-%d")
# except ValueError as e:
#     #error_log(e,official_store)
#     st.error("Date format are invalid, please use YYYY-mm-dd format")
#     expander = st.expander("Show details")
#     expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['start_date','end_date']))
#     exit()
# for fields in numerical_column:
#     numerical_validation(df,fields)
# e_index = []
# for i in range(len(df)):
#     if len(df.loc[i,'product_code'])<6:
#         e_index.append(i)
# if e_index !=[]:
#     st.error("Please check this product_code below, Are you sure the product_code is less than 6 character ?")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['product_code']))
#     exit()
# e_index = []
# for i in range(len(df)):
#     if df.loc[i,'rrp_incl_vat'] < df.loc[i,'rbp_incl_vat']:
#         e_index.append(i)
# if e_index !=[]:
#     st.error("RBP should be less than RRP")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['rrp_incl_vat','rbp_incl_vat']))
#     exit()
# e_index = []
# for i in range(len(df)):
#     if df.loc[i,'end_date'] < df.loc[i,'start_date']:
#         e_index.append(i)
# if e_index !=[]:
#     st.error("start_date > end_date")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['start_date','end_date']))
#     exit()
# e_index = []
# for i in range(len(df)):
#     if df.loc[i,'product_type']!='Gimmick' and (df.loc[i,'rrp_incl_vat']==0 or df.loc[i,'rbp_incl_vat']==0) :
#         e_index.append(i)
# if e_index !=[]:
#     st.error("RRP or RBP value can't be 0 for non-Gimmick Product")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['product_type','rrp_incl_vat','rbp_incl_vat']))
#     exit()
# e_index = []
# for i in range(len(df)):
#     if df.loc[i,'product_type'] not in product_type:
#         e_index.append(i)
# if e_index !=[]:
#     st.error("product_type should be Assembly, Single or Gimmick")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['product_type']))
#     exit()
# e_index = []
# for i in range(len(df)):
#     if df.loc[i,'principal'] not in principal_list:
#         e_index.append(i)
# if e_index !=[]:
#     st.error("Principal are not registered, please ensure that it has been registered [here](https://docs.google.com/spreadsheets/d/1rojNyRsFXNfzqT5flAVcXMJbg-dj2C0bVTV38PzmQco/edit#gid=1306233906)")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['principal']))
#     exit()
# e_index = []
# for i in range(len(df)):
#     if df.loc[i,'official_store'] not in os_list:
#         e_index.append(i)
# if e_index !=[]:
#     st.error("Official Store are not registered, please ensure that it has been registered [here](https://docs.google.com/spreadsheets/d/1rojNyRsFXNfzqT5flAVcXMJbg-dj2C0bVTV38PzmQco/edit#gid=1306233906)")
#     expander = st.expander("Show details")
#     expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['official_store']))
#     exit()
# totalrow = len(df)
# st.subheader("Validation Completed")
# expander = st.expander("show details")
# expander.write("%s/%s rows of data are validated"%(totalrow,totalrow))
# expander.dataframe(df)
# df['product_type']=df['product_type'].str.title()
# df['OS'] = official_store
# df['create_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %X')
# df['create_date'] = pd.to_datetime(df['create_date'])
# for i in range (len(df)):
#     df.loc[i,'uid']=datetime.datetime.utcnow().strftime('%Y%m-%d%H-%M%S-') + str(uuid4())
# #file_container = st.expander('Data Types')
# #file_container.text(df.dtypes)
# if st.button('Upload'):
#     try:
#         upload_bq(df)
#     except ValueError as e:
#         #error_log(e,official_store)
#         st.error('Upload failed,%s'%e)
#     else:
#         st.write('Upload Success!')
# else:
#     exit()
