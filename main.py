import datetime, time
import streamlit as st
import pandas as pd
import numpy as np
import datatest as dt
import json
import requests
from google.oauth2 import service_account
from google.cloud import storage

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = storage.Client(credentials=credentials)
exportbucket = client.get_bucket('streamlit-bucket-testing')


#Function to determine point seperator
def get_ps(df,i):
    ps = "."
    ts = ","
    if df.loc[i].find('.')!=-1:
        (ps,ts) = (',','.') if len(df.loc[i].split('.')[1])>=3 else ('.',',') 
    elif df.loc[i].find(',')!=-1 :
        (ps,ts) = ('.',',') if len(df.loc[i].split(',')[1])>=3 else (',','.')
    return df.loc[i].replace(ps,'koma').replace(ts,',').replace(',','').replace('koma','.')

def read_data(df):
    df = pd.read_csv(df)
    return df

st.title("RRP, RBP, and Promo Plan")

string_to_replace_dict = {"rp.":"","rp":""}


uploaded_file = st.file_uploader(label='')
if uploaded_file is None :
    st.error("Upload a .csv file first: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
else :
    try:
        df = read_data(uploaded_file)
        dt.validate(df.columns,{'principal', 'brand', 'product_code', 'product_description',
        'product_type', 'marketplace', 'official_store', 'start_date',
        'end_date', 'activity', 'rrp_incl_vat', 'promo_discount_percent',
        'promo_discount_amount', 'rrp_promo_incl_vat', 'rbp_incl_vat'})    
    except ValueError as e:
        st.error("The file should be in .csv format, use this template: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
    except dt.ValidationError as e:
        st.error("Invalid schema, please use this schema/template: [Input Data.csv](https://docs.google.com/spreadsheets/d/1eNKYbBJ-FKBM-y4QDu4BiyqnywqgXIFRujABblVqWXc/edit#gid=0)")
        headers = {
        "selector": "th:not(.index_name)",
        "props": "background-color: red;"
        }
        expander = st.expander("Show details")
        expander.dataframe(df.style.set_table_styles([headers]).set_properties(**{'background-color': 'red'}))

    else :
        df['promo_discount_percent'].fillna(0,inplace=True)
        df['promo_discount_amount'].fillna(0,inplace=True)
        df['rrp_promo_incl_vat'].fillna(0,inplace=True)
        if df.loc[:].isnull().values.any()==True:
            total = len(df)
            invalid_rows = [index for index, row in df.loc[:].iterrows() if row.isnull().any()]
            invld = len(invalid_rows)
            validation = ('Invalid Row '+str(invld)+'/'+str(total))
            empty = df.columns[df.isna().any()].tolist()
            st.error("%s field cannot be empty"%(empty))
            expander = st.expander("Show details")
            expander.write(validation)
            expander.dataframe(df.loc[invalid_rows].style.highlight_null(null_color='red'))    
        else:
            try:
                df['start_date']=pd.to_datetime(df['start_date'],format="%Y-%m-%d")
                df['end_date']=pd.to_datetime(df['end_date'],format="%Y-%m-%d")
            except ValueError as e:
                st.error("Date format are invalid, please use YYYY-mm-dd format")
                expander = st.expander("Show details")
                expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['start_date','end_date']))   
            else:
                try :
                    if df['rrp_incl_vat'].dtypes == 'object':
                        df['rrp_incl_vat']=df['rrp_incl_vat'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                        for i in range (len(df)):
                            df['rrp_incl_vat'].loc[i] = get_ps(df['rrp_incl_vat'],i)
                        df['rrp_incl_vat'] = df['rrp_incl_vat'].astype(float)
                    else :
                        df['rrp_incl_vat'] = df['rrp_incl_vat'].astype(float)
                except ValueError as e:
                    st.error("Can't convert 'rrp_incl_vat' column to the correct format")
                    expander = st.expander("Show details")
                    expander.error(e)
                    expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['rrp_incl_vat']))
                else : 
                    try:
                        if df['promo_discount_percent'].dtypes == 'object':
                            df['promo_discount_percent']=df['promo_discount_percent'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                            for i in range (len(df)):
                                df['promo_discount_percent'].loc[i] = get_ps(df['promo_discount_percent'],i)
                            df['promo_discount_percent'] = df['promo_discount_percent'].astype(float)
                        else :
                            df['promo_discount_percent'] = df['promo_discount_percent'].astype(float)
                    except ValueError as e:
                        st.error("Can't convert 'promo_discount_percent' column to the correct format")
                        expander = st.expander("Show details")
                        expander.error(e)
                        expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['promo_discount_percent']))
                    else : 
                        try:
                            if df['promo_discount_amount'].dtypes == 'object':
                                df['promo_discount_amount']=df['promo_discount_amount'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                                for i in range (len(df)):
                                    df['promo_discount_amount'].loc[i] = get_ps(df['promo_discount_amount'],i)
                                df['promo_discount_amount'] = df['promo_discount_amount'].astype(float)
                            else :
                                df['promo_discount_amount'] = df['promo_discount_amount'].astype(float)
                        except ValueError as e:
                            st.error("Can't convert 'promo_discount_amount' column to the correct format")
                            expander = st.expander("Show details")
                            expander.error(e)
                            expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['promo_discount_amount']))
                        else : 
                            try:
                                if df['rrp_promo_incl_vat'].dtypes == 'object':
                                    df['rrp_promo_incl_vat']=df['rrp_promo_incl_vat'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                                    for i in range (len(df)):
                                        df['rrp_promo_incl_vat'].loc[i] = get_ps(df['rrp_promo_incl_vat'],i)
                                    df['rrp_promo_incl_vat'] = df['rrp_promo_incl_vat'].astype(float)
                                else :
                                    df['rrp_promo_incl_vat'] = df['rrp_promo_incl_vat'].astype(float)
                            except ValueError as e:
                                st.error("Can't convert 'rrp_promo_incl_vat' column to the correct format")
                                expander = st.expander("Show details")
                                expander.error(e)
                                expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['rrp_promo_incl_vat']))
                            else : 
                                try:
                                    if df['rbp_incl_vat'].dtypes == 'object':
                                        df['rbp_incl_vat']=df['rbp_incl_vat'].astype(str).str.lower().replace(string_to_replace_dict,regex=True)
                                        for i in range (len(df)):
                                            df['rbp_incl_vat'].loc[i] = get_ps(df['rbp_incl_vat'],i)
                                        df['rbp_incl_vat'] = df['rbp_incl_vat'].astype(float)
                                    else :
                                        df['rbp_incl_vat'] = df['rbp_incl_vat'].astype(float)
                                except ValueError as e:
                                    st.error("Can't convert 'rbp_incl_vat' column to the correct format, %s" %e)
                                    expander = st.expander("Show details")
                                    expander.error(e)
                                    expander.dataframe(df.style.set_properties(**{'background-color': 'red'},subset=['rbp_incl_vat']))  
                                else : 
                                    totalrow = len(df)
                                    st.subheader("Validation Completed")
                                    expander = st.expander("show details")
                                    expander.write("%s/%s rows of data are validated"%(totalrow,totalrow))
                                    expander.dataframe(df)  
                                    #file_container = st.expander('Data Types')
                                    #file_container.text(df.dtypes)
                                    if st.button('Upload'):
                                        try:
                                            exportbucket.blob('test-local{0}.csv'.format(datetime.datetime.now().strftime('%Y-%m-%d'))).upload_from_string(df.to_csv(),'csv')
                                        except ValueError as e:
                                            st.error('Upload failed')
                                        else:
                                            st.write('Upload Success!')
                                    else:
                                        st.write('')
