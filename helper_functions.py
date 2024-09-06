import pandas as pd

def get_file_type(file):
    
    if file.name.endswith(".csv"):
        return "csv"
    elif file.name.endswith(".xlsx"):
        return "xlsx"
    elif file.name.endswith(".xls"):
        return 'xls'
    
def processing_dks_inova(raw_mow, raw_tkm, catalog):

    # Keep relevant columns
    keep_columns = ['Channel', 'Status', 'Fecha', 'Total Products', 'Total Descuento', 'Familia de Producto', 'Orden']

    raw_mow = raw_mow[keep_columns]
    raw_tkm = raw_tkm[keep_columns]

    # Join dfs
    df = pd.concat([raw_mow, raw_tkm])

    # Filtering and processing
    df = df[(df['Status'] != 'Cancelled') & (df['Status'] != 'Void')]
    df['Total Order'] = df['Total Products'] - df['Total Descuento']

    # Create All Inova category
    all_inova = df.groupby(['Channel', 'Fecha']).agg(Total_Order = ('Total Order', 'sum'), Orders = ('Orden', 'size')).reset_index()
    all_inova.columns = ['Channel', 'Date', 'Total Order', 'Orders']
    all_inova['Product family'] = 'ALL INOVA'

    all_inova = pd.merge(all_inova, catalog[['ORIGEN DE VENTA', 'CANAL']], how = 'left', left_on = 'Channel', right_on = 'ORIGEN DE VENTA')
    all_inova = all_inova[['Date', 'CANAL', 'Product family', 'Total Order', 'Orders']]
    all_inova.columns = ['Date', 'Channel', 'Product family', 'Total', 'Orders']

    all_inova = all_inova.groupby(['Date', 'Channel', 'Product family'])[['Total', 'Orders']].sum().reset_index()

    # By product
    keep_products = ['EAGLE EYES', 'GREEN MARVEL', 'GREEN MARVEL LEGS', 'ROTAFLEX', 'SKOON', 'TERRACOAT BATERIA', 'XTENDER', 'XSHOCK', 'XSHOCK VORTEX', 'UROCAPS']
    df = df[df['Familia de Producto'].isin(keep_products)]

    df = pd.merge(df, catalog[['ORIGEN DE VENTA', 'CANAL']], how = 'left', left_on = 'Channel', right_on = 'ORIGEN DE VENTA')
    df = df[['Fecha', 'Orden', 'CANAL', 'Familia de Producto', 'Total Order']]
    df.columns = ['Date', 'Order', 'Channel', 'Product family', 'Total']

    # Group
    df = df.groupby(['Date', 'Product family', 'Channel']).agg(Orders = ('Order', 'size'), Total = ('Total', 'sum')).reset_index()

    # Join All Inova and specific products
    df = pd.concat([df, all_inova])

    # Define all possible products and channels
    all_days = df['Date'].unique()
    all_products = ['ALL INOVA'] + keep_products
    all_channels = catalog['CANAL'].unique()

    # Create a MultiIndex DataFrame with all combinations of products and channels
    multi_index = pd.MultiIndex.from_product([all_days, all_products, all_channels], names = ['Date', 'Product family', 'Channel'])

    # Reindex the original DataFrame to this new index
    df_reindexed = df.set_index(['Date', 'Product family', 'Channel']).reindex(multi_index, fill_value = 0).reset_index()
    df_reindexed['Product_Channel'] = df_reindexed['Product family'] + ' - ' + df_reindexed['Channel']

    orders_pivot = df_reindexed.pivot(index = 'Date', columns = 'Product_Channel', values = 'Orders')
    revenue_pivot = df_reindexed.pivot(index = 'Date', columns = 'Product_Channel', values = 'Total')

    # Combine Orders and Revenue into a single DataFrame
    result = pd.concat([orders_pivot.add_suffix(' - Orders'), revenue_pivot.add_suffix(' - Revenue')], axis = 1).reset_index()

    return df, result
    
def processing_amazon_sellerboard(amz_df, amz_listing_df, date = False):

    amz_listing_df = amz_listing_df[['ASIN', 'PRODUCT', 'NAME', 'SKU', 'BRAND', 'PRODUCTO']]
    amz_listing_df = amz_listing_df.rename(columns = {'PRODUCTO': 'Type of product', 'BRAND': 'Brand'})

    if date:
        amz_df = amz_df[['FECHA', 'Product', 'ASIN', 'SKU', 'Units', 'Sales', 'Ads']]
    else:
        amz_df = amz_df[['Product', 'ASIN', 'SKU', 'Units', 'Sales', 'Ads']]

    amz_df['Ads'] = amz_df['Ads'].abs()

    amz_listing_merge = amz_listing_df[['SKU', 'Brand', 'Type of product']]
    amz_df = pd.merge(amz_df, amz_listing_merge, on = 'SKU', how = 'left').drop_duplicates()

    na_mask = amz_df['Brand'].isna() | amz_df['Type of product'].isna()

    if date:
        amz_na_df = amz_df[na_mask][['FECHA', 'Product', 'ASIN', 'SKU', 'Units', 'Sales', 'Ads']]
        amz_grouped_df = amz_df.groupby(['FECHA', 'Brand', 'Type of product'])[['Ads', 'Units', 'Sales']].sum()
    else:
        amz_na_df = amz_df[na_mask][['Product', 'ASIN', 'SKU', 'Units', 'Sales', 'Ads']]
        amz_grouped_df = amz_df.groupby(['Brand', 'Type of product'])[['Ads', 'Units', 'Sales']].sum()

    return amz_grouped_df, amz_na_df

def processing_amazon_sellercentral(amz_df, amz_listing_df, date = False):

    amz_listing_df = amz_listing_df[['ASIN', 'PRODUCT', 'NAME', 'SKU', 'BRAND', 'PRODUCTO']]
    amz_listing_df = amz_listing_df.rename(columns = {'PRODUCTO': 'Type of product', 'BRAND': 'Brand'})

    if date:
        amz_df = amz_df[['FECHA', 'ASIN (parent)', 'ASIN (child)', 'Título', 'SKU', 'Sesiones: total', 'Vistas de página: total']]
    else:
        amz_df = amz_df[['ASIN (parent)', 'ASIN (child)', 'Título', 'SKU', 'Sesiones: total', 'Vistas de página: total']]

    try:
        amz_df['Sesiones: total'] = amz_df['Sesiones: total'].str.replace(',', '').astype(int)
    except:
        pass

    try:
        amz_df['Vistas de página: total'] = amz_df['Vistas de página: total'].str.replace(',', '').astype(int)
    except:
        pass

    amz_listing_merge = amz_listing_df[['SKU', 'Brand', 'Type of product']]
    amz_df = pd.merge(amz_df, amz_listing_merge, on = 'SKU', how = 'left').drop_duplicates()

    na_mask = amz_df['Brand'].isna() | amz_df['Type of product'].isna()

    if date:
        amz_na_df = amz_df[na_mask][['FECHA', 'ASIN (parent)', 'ASIN (child)', 'Título', 'SKU', 'Sesiones: total', 'Vistas de página: total']]
        amz_grouped_df = amz_df.groupby(['FECHA', 'Brand', 'Type of product'])[['Sesiones: total', 'Vistas de página: total']].sum()
    else:
        amz_na_df = amz_df[na_mask][['ASIN (parent)', 'ASIN (child)', 'Título', 'SKU', 'Sesiones: total', 'Vistas de página: total']]
        amz_grouped_df = amz_df.groupby(['Brand', 'Type of product'])[['Sesiones: total', 'Vistas de página: total']].sum()

    return amz_grouped_df, amz_na_df
