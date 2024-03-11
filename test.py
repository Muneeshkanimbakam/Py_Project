import streamlit as st
import pandas as pd
import concurrent.futures
import logging
import cx_Oracle
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define SQL query
SQL_QUERY = """
SELECT count(DISTINCT product_id) AS No_of_Product_IDs, count(customer_id) AS No_Of_BANs
FROM product
WHERE customer_id IN (
    SELECT b.ban
    FROM billing_account b
    JOIN address_name_link a ON b.ban = a.ban
    JOIN address_data d ON a.address_id = d.address_id
    WHERE b.company_owner_id = '4'
        AND b.ban_status = 'O'
        AND a.expiration_date IS NULL
        AND a.link_type = 'B'
        AND d.adr_state_code = :state_code
)
"""

def fetch_data(state_code):
    try:
        # Replace with your Oracle connection details
        db_user = 'cuappc'
        db_password = 'ogspet1'
        db_host = 'lxmdtsdxpet-scan.test.intranet'
        db_port = '1521'
        db_service_name = 'ENSPET'

        # Create SQLAlchemy engine
        db_url = f"oracle+cx_oracle://{db_user}:{db_password}@{db_host}:{db_port}/{db_service_name}"
        engine = create_engine(db_url)

        # Execute SQL query
        df = pd.read_sql(SQL_QUERY, engine, params={'state_code': state_code})
        return df
    except Exception as error:
        logger.error(f"Error executing SQL query: {error}")
        return None

def display_state_data(state_code):
    st.subheader(f'Table data for {state_code}')
    df = fetch_data(state_code)
    if df is not None:
        st.table(df.head(100))
    else:
        st.warning(f"No data found for {state_code}")

def main():
    st.title('Data Fetching App')
    state_codes = st.text_input('Enter State Codes (comma-separated):')
    state_codes_list = [code.strip() for code in state_codes.split(',')]

    if state_codes_list:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(display_state_data, state_code) for state_code in state_codes_list]
            concurrent.futures.wait(futures)

if __name__ == '__main__':
    main()
