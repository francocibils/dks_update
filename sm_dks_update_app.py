import streamlit as st
import pandas as pd

from io import BytesIO
from xlsxwriter import Workbook
from helper_functions import *

app_mode = st.sidebar.selectbox('Select source to upload', ['DKS - Inova', 'DKS - Sognare'])

if app_mode == 'DKS - Inova':
    st.title('DKS - Inova')
    st.header('File upload')
    st.markdown('Upload file to obtain orders and revenue for each Brand and Channel of sale.')

    raw_mow = st.file_uploader('Upload DKS - MOW file', type = ['xlsx', 'xls', 'csv'])
    raw_tkm = st.file_uploader('Upload DKS - TKM file', type = ['xlsx', 'xls', 'csv'])

    catalog = st.file_uploader('Upload Catalog file (optional)', type = ['xlsx'])

    if raw_mow is not None:
        file_type = get_file_type(raw_mow)
        
        if file_type == 'csv':
            mow_df = pd.read_csv(raw_mow, encoding = 'latin-1')
        elif file_type == 'xlsx' or file_type == 'xls':
            mow_df = pd.read_excel(raw_mow, encoding = 'latin-1')
        
        st.success('DKS - MOW file uploaded successfully.')

    if raw_tkm is not None:
        file_type = get_file_type(raw_tkm)
        
        if file_type == 'csv':
            tkm_df = pd.read_csv(raw_tkm, encoding = 'latin-1')
        elif file_type == 'xlsx' or file_type == 'xls':
            tkm_df = pd.read_excel(raw_tkm, encoding = 'latin-1')
        
        st.success('DKS - TKM file uploaded successfully.')

    if catalog is not None:
        catalog_df = pd.read_excel(catalog, engine = 'openpyxl')
        st.success('Catalog file uploaded successfully.')
    else:
        catalog_df = pd.read_excel(r'https://raw.githubusercontent.com/francocibils/dks_update/main/Catalog%20DKS%20MX.xlsx', engine = 'openpyxl')
        st.info('Default catalog file used.')

    if st.button('Process file'):

        dks_pivot, dks_sm = processing_dks_inova(mow_df, tkm_df, catalog_df)
        dks_payment = processing_dks_inova_payment(mow_df, tkm_df, catalog_df)

        st.header('Processed data')
        st.success('DKS files have been processed successfully.')
        
        # Convert the DataFrame to an Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine = 'xlsxwriter') as writer:
            dks_sm.to_excel(writer, index = False, sheet_name = 'Supermetrics table')
            dks_pivot.to_excel(writer, index = False, sheet_name = 'Pivot table')
            dks_payment.to_excel(writer, index = True, sheet_name = 'Payment table')
            writer.close()

        # Rewind the buffer
        output.seek(0)

        # Create a download button
        st.download_button(
            label = "Download Excel file",
            data = output,
            file_name = "DKS.xlsx",
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if app_mode == 'DKS - Sognare':
    st.title('DKS - Sognare')
    st.header('File upload')
    st.markdown('Upload file to obtain orders and revenue for each Brand and Channel of sale.')

    raw_df = st.file_uploader('Upload DKS - Sognare file', type = ['xlsx', 'xls', 'csv'])
    raw_mow = st.file_uploader('Upload DKS - MOW file', type = ['xlsx', 'xls', 'csv'])
    raw_tkm = st.file_uploader('Upload DKS - TKM file', type = ['xlsx', 'xls', 'csv'])

    # Import catalog
    catalog_product = pd.read_excel(r'https://raw.githubusercontent.com/francocibils/dks_update/main/Catalog%20DKS%20Sognare%20-%20Product.xlsx', engine = 'openpyxl')
    catalog_channel = pd.read_excel(r'https://raw.githubusercontent.com/francocibils/dks_update/main/Catalog%20DKS%20Sognare%20-%20Channel.xlsx', engine = 'openpyxl')
    catalog_mx = pd.read_excel(r'https://raw.githubusercontent.com/francocibils/dks_update/main/Catalog%20DKS%20MX.xlsx', engine = 'openpyxl')
    
    if raw_df is not None:
        file_type = get_file_type(raw_df)
        
        if file_type == 'csv':
            sognare_df = pd.read_csv(raw_df, encoding = 'latin-1')
        elif file_type == 'xlsx' or file_type == 'xls':
            sognare_df = pd.read_excel(raw_df)
        
        st.success('DKS - Sognare file uploaded successfully.')

    if raw_mow is not None:
        file_type = get_file_type(raw_mow)
        
        if file_type == 'csv':
            mow_df = pd.read_csv(raw_mow, encoding = 'latin-1')
        elif file_type == 'xlsx' or file_type == 'xls':
            mow_df = pd.read_excel(raw_mow)
        
        st.success('DKS - MOW file uploaded successfully.')

    if raw_tkm is not None:
        file_type = get_file_type(raw_tkm)
        
        if file_type == 'csv':
            tkm_df = pd.read_csv(raw_tkm, encoding = 'latin-1')
        elif file_type == 'xlsx' or file_type == 'xls':
            tkm_df = pd.read_excel(raw_tkm)
        
        st.success('DKS - TKM file uploaded successfully.')

    if st.button('Process file'):

        dks_pivot, dks_sm = processing_dks_sognare(sognare_df, catalog_product, catalog_channel, add_inova_products = [mow_df, tkm_df, catalog_mx])

        st.header('Processed data')
        st.success('DKS files have been processed successfully.')
        
        # Convert the DataFrame to an Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine = 'xlsxwriter') as writer:
            dks_sm.to_excel(writer, index=False, sheet_name = 'Supermetrics table')
            dks_pivot.to_excel(writer, index=False, sheet_name = 'Pivot table')
            writer.close()

        # Rewind the buffer
        output.seek(0)

        # Create a download button
        st.download_button(
            label = "Download Excel file",
            data = output,
            file_name = "DKS.xlsx",
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
