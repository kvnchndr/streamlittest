import streamlit as st
import pandas as pd
import numpy as np

#Function to determine point seperator
def get_ps(df,i):
  ps = "."
  ts = ","
  if df.loc[i].find('.')!=-1:
    (ps,ts) = (',','.') if len(df.loc[i].split('.')[1])>=3 else ('.',',')
  elif df.loc[i].find(',')!=-1 :
    (ps,ts) = ('.',',') if len(df.loc[i].split(',')[1])>=3 else (',','.')
  return df.loc[i].replace(ps,'koma').replace(ts,',').replace(',','').replace('koma','.')


st.title("RRP Collector")

try:
    uploaded_file = st.file_uploader(label='')
    df = pd.read_csv(uploaded_file) 

except ValueError:
    st.error(
            f"""
                👆 Upload a .csv file first. Sample to try: [RRP-input.csv](https://drive.google.com/file/d/1_DFDkkE-1Pjl0FIW89LAGShez-OoD3c9/view?usp=sharing)
                """
    )
else :
    if df.loc[:, df.columns != 'discount_type'].isnull().values.any()==True:
        total = len(df)
        invalid_rows = [index for index, row in df.loc[:, df.columns != 'discount_type'].iterrows() if row.isnull().any()]
        invld = len(invalid_rows)
        validation = ('Invalid Row '+str(invld)+'/'+str(total))
        file_container = st.expander(validation)
        file_container.write(df.loc[invalid_rows])
    else:
        try:
            df['start_date']=pd.to_datetime(df['start_date'],format="%Y-%m-%d")
            df['end_date']=pd.to_datetime(df['end_date'],format="%Y-%m-%d")
        except ValueError as e:
            st.error("Date Format are Invalid, use %Y-%m-%d format") 
        else:
            try :
                if df['buy_price_incl_vat'].dtypes == 'object':
                    df['buy_price_incl_vat']=df['buy_price_incl_vat'].astype(str).str.lower().replace({"rp.":"","rp":""},regex=True)
                    for i in range (len(df)):
                        df['buy_price_incl_vat'].loc[i] = get_ps(df['buy_price_incl_vat'],i)
                    df['buy_price_incl_vat'] = df['buy_price_incl_vat'].astype(float)
                else :
                    df['buy_price_incl_vat'] = df['buy_price_incl_vat'].astype(float)
            except ValueError as e:
                st.error("Can't convert 'buy_price_incl_vat' column to the correct format, %s" %e)
            else : 
                try:
                    if df['retail_price_incl_vat'].dtypes == 'object':
                        df['retail_price_incl_vat']=df['retail_price_incl_vat'].astype(str).str.lower().replace({"rp.":"","rp":""},regex=True)
                        for i in range (len(df)):
                            df['retail_price_incl_vat'].loc[i] = get_ps(df['retail_price_incl_vat'],i)
                        df['retail_price_incl_vat'] = df['retail_price_incl_vat'].astype(float)
                    else :
                        df['retail_price_incl_vat'] = df['retail_price_incl_vat'].astype(float)
                except ValueError as e:
                    st.error("Can't convert 'retail_price_incl_vat' column to the correct format, %s" %e)
                else : 
                    try:
                        if df['buy_price_promo_include_vat'].dtypes == 'object':
                            df['buy_price_promo_include_vat']=df['buy_price_promo_include_vat'].astype(str).str.lower().replace({"rp.":"","rp":""},regex=True)
                            for i in range (len(df)):
                                df['buy_price_promo_include_vat'].loc[i] = get_ps(df['buy_price_promo_include_vat'],i)
                            df['buy_price_promo_include_vat'] = df['buy_price_promo_include_vat'].astype(float)
                        else :
                            df['buy_price_promo_include_vat'] = df['buy_price_promo_include_vat'].astype(float)
                    except ValueError as e:
                        st.error("Can't convert 'buy_price_promo_include_vat' column to the correct format, %s" %e)
                    else : 
                        df['buy_price_promo_include_vat'] = df['buy_price_promo_include_vat'].astype(float)
                        st.subheader("Data 100% Validated")
    st.write(df)
    st.subheader("Data Types")
    st.text(df.dtypes)
