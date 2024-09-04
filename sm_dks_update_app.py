import streamlit as st
import pandas as pd
from io import BytesIO

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

if app_mode == 'DKS - Sognare':
    pass
