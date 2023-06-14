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
    errors = pd.DataFrame({"message":['oke'],"OS":["OS"],"created_time":["time"]})
    errors["message"]=e
    errors["OS"]=official_store
    errors["created_time"]=datetime.datetime.utcnow().strftime('%Y-%m-%d %X')
    errors["created_time"]=pd.to_datetime(errors["created_time"])
    target_table = 'marketplace.streamlit_error_log'
    project_id = credentials.project_id
    errors.to_gbq(target_table, project_id=project_id, if_exists='append', progress_bar=True, credentials=credentials)

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


st.title("RRP, RBP, and Promo Plan")

string_to_replace_dict = {"rp.":"","rp":""}
SHEET_ID = '1ipc6iMh9adYIFcqzg_wnVt3ftzAUjCng7Z6fP5z6Ib4'
SHEET_NAME = 'os'
url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
df = pd.read_csv(url)
a = ",".join(df['official_store'])
official_store = st.selectbox('Please choose the official Store',a.split(','))


uploaded_file = st.file_uploader(label='Upload Input Files',label_visibility='collapsed')
if uploaded_file is None :
    st.error("Upload a .csv file first: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
else :
    try:
        df = pd.read_csv(uploaded_file)
        dt.validate(df.columns,{'principal', 'brand', 'product_code', 'product_description',
        'product_type', 'marketplace', 'official_store', 'start_date',
        'end_date', 'activity','promo_campaign_name', 'rrp_incl_vat', 'promo_discount_percent',
        'promo_discount_amount', 'rrp_promo_incl_vat', 'rbp_incl_vat'})
        for i in range(len(df)):
            if df.loc[i,'activity'] == 'Non-Promo' or df.loc[i,'activity'] == 'Non Promo':
                df.loc[i,'promo_campaign_name'] = 'Non-Promo'
            else : 
                df.loc[i,'promo_campaign_name'] = df.loc[i,'promo_campaign_name']  
    except ValueError as e:
        #error_log(e,official_store)
        st.error("The file should be in .csv format, use this template: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
    except dt.ValidationError as e:
        #error_log(e,official_store)
        st.error("Invalid schema, please use this schema/template: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
        headers = {
        "selector": "th:not(.index_name)",
        "props": "background-color: red;"
        }
        expander = st.expander("Show details")
        expander.dataframe(df.style.set_table_styles([headers]).set_properties(**{'background-color': 'red'}))

    else :
        df = df.dropna(axis = 0, how = 'all').reset_index(drop=True)
        df['promo_discount_percent'].fillna(0,inplace=True)
        df['promo_discount_amount'].fillna(0,inplace=True)
        df['rrp_promo_incl_vat'].fillna(0,inplace=True)
        if df.loc[:].isnull().values.any()==True:
            total = len(df)
            invalid_rows = [index for index, row in df.loc[:].iterrows() if row.isnull().any()]
            invld = len(invalid_rows)
            validation = ('Invalid Row '+str(invld)+'/'+str(total))
            empty = df.columns[df.isna().any()].tolist()
            e = "%s field cannot be empty"%(empty)
            #error_log(e,official_store)
            st.error("%s field cannot be empty"%(empty))
            expander = st.expander("Show details")
            expander.write(validation)
            expander.dataframe(df.loc[invalid_rows].style.highlight_null(color='red'))    
        else:
            try:
                df['start_date']=pd.to_datetime(df['start_date'],format="%Y-%m-%d")
                df['end_date']=pd.to_datetime(df['end_date'],format="%Y-%m-%d")
            except ValueError as e:
                #error_log(e,official_store)
                st.error("Date format are invalid, please use YYYY-mm-dd format")
                expander = st.expander("Show details")
                expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['start_date','end_date']))   
            else:
                try :
                    if df['rrp_incl_vat'].dtypes == 'object':
                        df['rrp_incl_vat']=df['rrp_incl_vat'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                        for i in range (len(df)):
                            df.loc[i,'rrp_incl_vat'] = get_ps(df['rrp_incl_vat'],i)
                        df['rrp_incl_vat'] = df['rrp_incl_vat'].astype(float).abs()
                    else :
                        df['rrp_incl_vat'] = df['rrp_incl_vat'].astype(float).abs()
                except ValueError as e:
                    #error_log(e,official_store)
                    st.error("Can't convert 'rrp_incl_vat' column to the correct format")
                    expander = st.expander("Show details")
                    df2 = findtext('rrp_incl_vat',e)
                    expander.error(e)
                    expander.dataframe(df2.style.set_properties(**{'background-color': 'red'},subset=['rrp_incl_vat']))
                else : 
                    try:
                        if df['promo_discount_percent'].dtypes == 'object':
                            df['promo_discount_percent']=df['promo_discount_percent'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                            for i in range (len(df)):
                                df.loc[i,'promo_discount_percent'] = get_ps(df['promo_discount_percent'],i)
                            df['promo_discount_percent'] = df['promo_discount_percent'].astype(float).abs()
                        else :
                            df['promo_discount_percent'] = df['promo_discount_percent'].astype(float).abs()
                    except ValueError as e:
                        #error_log(e,official_store)
                        st.error("Can't convert 'promo_discount_percent' column to the correct format")
                        expander = st.expander("Show details")
                        df2 = findtext('promo_discount_percent',e)
                        expander.error(e)
                        expander.dataframe(df2.style.set_properties(**{'background-color': 'red'},subset=['promo_discount_percent']))
                    else : 
                        try:
                            if df['promo_discount_amount'].dtypes == 'object':
                                df['promo_discount_amount']=df['promo_discount_amount'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                                for i in range (len(df)):
                                    df.loc[i,'promo_discount_amount'] = get_ps(df['promo_discount_amount'],i)
                                df['promo_discount_amount'] = df['promo_discount_amount'].astype(float).abs()
                            else :
                                df['promo_discount_amount'] = df['promo_discount_amount'].astype(float).abs()
                        except ValueError as e:
                            #error_log(e,official_store)
                            st.error("Can't convert 'promo_discount_amount' column to the correct format")
                            expander = st.expander("Show details")
                            df2 = findtext('promo_discount_amount',e)
                            expander.error(e)
                            expander.dataframe(df2.style.set_properties(**{'background-color': 'red'},subset=['promo_discount_amount']))
                        else : 
                            try:
                                if df['rrp_promo_incl_vat'].dtypes == 'object':
                                    df['rrp_promo_incl_vat']=df['rrp_promo_incl_vat'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                                    for i in range (len(df)):
                                        df.loc[i,'rrp_promo_incl_vat']  = get_ps(df['rrp_promo_incl_vat'],i)
                                    df['rrp_promo_incl_vat'] = df['rrp_promo_incl_vat'].astype(float).abs()
                                else :
                                    df['rrp_promo_incl_vat'] = df['rrp_promo_incl_vat'].astype(float).abs()
                            except ValueError as e:
                                #error_log(e,official_store)
                                st.error("Can't convert 'rrp_promo_incl_vat' column to the correct format")
                                expander = st.expander("Show details")
                                df2 = findtext('rrp_promo_incl_vat',e)
                                expander.error(e)
                                expander.dataframe(df2.style.set_properties(**{'background-color': 'red'},subset=['rrp_promo_incl_vat']))
                            else : 
                                try:
                                    if df['rbp_incl_vat'].dtypes == 'object':
                                        df['rbp_incl_vat']=df['rbp_incl_vat'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                                        for i in range (len(df)):
                                            df.loc[i,'rbp_incl_vat'] = get_ps(df['rbp_incl_vat'],i)
                                        df['rbp_incl_vat'] = df['rbp_incl_vat'].astype(float).abs()
                                    else :
                                        df['rbp_incl_vat'] = df['rbp_incl_vat'].astype(float).abs()
                                except ValueError as e:
                                    #error_log(e,official_store)
                                    st.error("Can't convert 'rbp_incl_vat' column to the correct format")
                                    expander = st.expander("Show details")
                                    df2 = findtext('rbp_incl_vat',e)
                                    expander.error(e)
                                    expander.dataframe(df2.style.set_properties(**{'background-color': 'red'},subset=['rbp_incl_vat']))  
                                else :
                                    e_index = []
                                    for i in range(len(df)):
                                        if len(df.loc[i,'product_code'])<6:
                                            e_index.append(i)
                                        else :
                                            continue
                                    if e_index !=[]:
                                        st.error("Please check this product_code below, Are you sure the product_code is less than 6 character ?")
                                        expander = st.expander("Show details")
                                        expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['product_code']))
                                    else:
                                        e_index = []
                                        for i in range(len(df)):
                                            if df.loc[i,'rrp_incl_vat'] < df.loc[i,'rbp_incl_vat']:
                                                e_index.append(i)
                                            else :
                                                continue
                                        if e_index !=[]:
                                            st.error("RBP should be less than RRP")
                                            expander = st.expander("Show details")
                                            expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['rrp_incl_vat','rbp_incl_vat'])) 
                                        else:
                                            e_index = []
                                            for i in range(len(df)):
                                                if df.loc[i,'end_date'] < df.loc[i,'start_date']:
                                                    e_index.append(i)
                                                else :
                                                    continue
                                            if e_index !=[]:
                                                st.error("start_date > end_date")
                                                expander = st.expander("Show details")
                                                expander.dataframe(df.loc[e_index].style.set_properties(**{'background-color': 'red'},subset=['start_date','end_date']))
                                            else :
                                                totalrow = len(df)
                                                st.subheader("Validation Completed")
                                                expander = st.expander("show details")
                                                expander.write("%s/%s rows of data are validated"%(totalrow,totalrow))
                                                expander.dataframe(df)
                                                for i in ('principal','brand','product_code','product_description','product_type','marketplace','official_store'):
                                                    df[i] = df[i].str.strip()
                                                df['product_type']=df['product_type'].str.title()
                                                df['OS'] = official_store
                                                df['create_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %X')
                                                df['create_date'] = pd.to_datetime(df['create_date'])
                                                for i in range (len(df)):
                                                    df.loc[i,'uid']=datetime.datetime.utcnow().strftime('%Y%m-%d%H-%M%S-') + str(uuid4())
                                                #file_container = st.expander('Data Types')
                                                #file_container.text(df.dtypes)
                                                if st.button('Upload'):
                                                    try:
                                                        #exportbucket.blob('test-local{0}.csv'.format(datetime.datetime.now().strftime('%Y-%m-%d'))).upload_from_string(df.to_csv(),'csv')
                                                        upload_bq(df)

                                                    except ValueError as e:
                                                        #error_log(e,official_store)
                                                        st.error('Upload failed,%s'%e)
                                                    else:
                                                        st.write('Upload Success!')
                                                else:
                                                    st.write('')
