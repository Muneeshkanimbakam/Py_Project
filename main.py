import streamlit as st
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
import logging
import datetime

logging.basicConfig(filename='logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
total_bans="""SELECT d.adr_state_code, COUNT(DISTINCT b.ban)
FROM billing_account b
INNER JOIN address_name_link a ON b.ban = a.ban
INNER JOIN address_data d ON a.address_id = d.address_id
WHERE b.company_owner_id = '4'
  AND b.ban_status NOT IN ('N', 'C', 'T')
  AND a.expiration_date IS NULL
  AND a.link_type = 'B'
  and d.adr_state_code=:state_code
GROUP BY d.adr_state_code
"""
SQL_QUERY = """SELECT
  COUNT(*) AS order_count,
  COUNT(DISTINCT v.om_product_id) AS distinct_product_count
FROM oms_order o
INNER JOIN oms_product_version v ON v.om_product_id = o.om_product_id
INNER JOIN billing_account ba ON v.ban_id = ba.ban
INNER JOIN address_name_link a ON ba.ban = a.ban
/*+ INDEX(address_name_link (ban)) */
INNER JOIN address_data d ON a.address_id = d.address_id
/*+ INDEX(address_data (address_id)) */
WHERE ba.company_owner_id = '4'
  AND d.adr_state_code = :state_code
  AND ba.ban_status NOT IN ('N', 'C', 'T')
  AND a.expiration_date IS NULL
  AND a.link_type = 'B'
AND o.om_order_status NOT IN ('CP', 'CA', 'AM') 
AND o.order_version_status = 'CU'
AND v.product_ver_status = 'CU'"""
DOWNLOAD_QUERY = """SELECT
  o.ban as BAN,
  v.billing_product_id as product_id,
  v.om_product_id AS om_product_id,
  o.om_order_id AS om_order_id,
  O.HOLD_IND AS HOLD_INDICATOR,
  o.om_order_status as order_status
FROM oms_order o
INNER JOIN oms_product_version v ON v.om_product_id = o.om_product_id
INNER JOIN billing_account ba ON v.ban_id = ba.ban
INNER JOIN address_name_link a ON ba.ban = a.ban
/*+ INDEX(address_name_link (ban)) */
INNER JOIN address_data d ON a.address_id = d.address_id
/*+ INDEX(address_data (address_id)) */
WHERE ba.company_owner_id = '4'
  AND d.adr_state_code = :state_code
  AND ba.ban_status NOT IN ('N', 'C', 'T')
  AND a.expiration_date IS NULL
  AND a.link_type = 'B'
AND o.om_order_status NOT IN ('CP', 'CA', 'AM') 
AND o.order_version_status = 'CU'
AND v.product_ver_status = 'CU'"""

def create_engine_with_cx_oracle():
    dsn_tns = cx_Oracle.makedsn('lxmdtsdxpet-scan.test.intranet', '1521', service_name='ENSPET')
    return create_engine(f'oracle+cx_oracle://cuappc:ogspet1@{dsn_tns}')

def fetch_data(state_code):
    engine = create_engine_with_cx_oracle()
    try:
        logging.info(f"Executing SQL query for state code: {state_code}")
        df = pd.read_sql_query(SQL_QUERY, engine, params={'state_code': state_code})
        return df
    except Exception as error:
        logging.error(f"Error executing SQL query: {error}")

def fetch_total_bans(state_code):
    engine = create_engine_with_cx_oracle()
    try:
        logging.info(f"Executing total_bans query for state code: {state_code}")
        total_bans_df = pd.read_sql_query(total_bans, engine, params={'state_code': state_code})
        logging.info(f"Total bans data fetched for state code: {state_code}")
        return total_bans_df
    except Exception as error:
        logging.error(f"Error executing SQL query: {error}")

def display_state_data(state_code):
    st.subheader(f"------ Data for {state_code} ------")
    with st.spinner("Fetching data..."):
        df = fetch_data(state_code)
        df2=fetch_total_bans(state_code)
        if df is not None:
            st.subheader(f'Total bans for {state_code}')
            st.table(df2.head(100))
            st.subheader(f'Data for {state_code}')
            st.table(df.head(100))
            logging.info(f"Total bans displayed for state code: {state_code}")
            logging.info(f"Data displayed for state code: {state_code}")
            st.success(f"Data fetched for state code: {state_code}")  # Notification
        else:
            st.warning(f"No data found for {state_code}")

def download_state_data(state_code):
    engine = create_engine_with_cx_oracle()
    try:
        logging.info(f"Downloading data for state code: {state_code}")
        df = pd.read_sql_query(DOWNLOAD_QUERY, engine, params={'state_code': state_code})
        if df is not None:
            df.to_csv(f"{state_code}_data.csv", index=False, mode='w')
            st.success(f"Data downloaded for {state_code}")
            st.table(df.head(100))
            logging.info(f"Data downloaded for state code: {state_code}")
        else:
            st.warning(f"No data found for {state_code}")
    except cx_Oracle.Error as error:
        logging.error(f"Error executing SQL query: {error}")

def download_data_thread(state_code):
    st.spinner(f"Downloading data for {state_code}...")
    logging.info(f"Downloading data for state code: {state_code}")
    download_state_data(state_code)
    st.success(f"Data downloaded for {state_code}")
    logging.info(f"Data downloaded for state code: {state_code}")

def display_data_thread(state_code):
    display_state_data(state_code)

def main():
    st.title('BRSD Data Fetching App')
    state_codes = st.text_input('Enter State Codes (comma-separated):')
    state_codes_list = [code.strip() for code in state_codes.split(',')]

    if state_codes_list:
        if st.button('Download Data'):
            for state_code in state_codes_list:
                state_code = state_code.upper()
                logging.info(f"Download button clicked for state code: {state_code}")
                download_state_data(state_code)

        if st.button('Display Data'):
            for state_code in state_codes_list:
                state_code = state_code.upper()
                logging.info(f"Display button clicked for state code: {state_code}")
                display_state_data(state_code)

if __name__ == '__main__':
    start = datetime.datetime.now()
    logging.info("-------------------------------New Session ----------------------------------")
    logging.info("Application started at %s", start)
    main()
    end = datetime.datetime.now()
    logging.info("Application exited at %s", end)
    logging.info("Application execution time: %s", end - start)
