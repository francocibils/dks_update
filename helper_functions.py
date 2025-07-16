import pandas as pd

def get_file_type(file):
    
    if file.name.endswith(".csv"):
        return "csv"
    elif file.name.endswith(".xlsx"):
        return "xlsx"
    elif file.name.endswith(".xls"):
        return 'xls'
    
def processing_dks_inova(raw_mow, raw_tkm, catalog, cobranza = True):

    # Keep relevant columns
    keep_columns = ['Channel', 'Status', 'Fecha', 'Total Products', 'Total Descuento', 'Familia de Producto', 'Orden']

    raw_mow = raw_mow[keep_columns]
    raw_tkm = raw_tkm[keep_columns]

    # Join dfs
    df = pd.concat([raw_mow, raw_tkm])

    # Filtering and processing
    if cobranza:
        df = df[(df['Status'] != 'Void') & (df['Status'] != 'Cancelled')]
        
    df['Total Order'] = df['Total Products'] - df['Total Descuento']

    # Create All Inova category
    temp_df = df[~df['Familia de Producto'].isin(['SOGNARE ALMOHADA BASE', 'EAGLE EYES'])]
    all_inova = temp_df.groupby(['Channel', 'Fecha']).agg(Total_Order = ('Total Order', 'sum'), Orders = ('Orden', 'size')).reset_index()
    all_inova.columns = ['Channel', 'Date', 'Total Order', 'Orders']
    all_inova['Product family'] = 'ALL INOVA'

    all_inova = pd.merge(all_inova, catalog[['ORIGEN DE VENTA', 'CANAL']], how = 'left', left_on = 'Channel', right_on = 'ORIGEN DE VENTA')
    all_inova = all_inova[['Date', 'CANAL', 'Product family', 'Total Order', 'Orders']]
    all_inova.columns = ['Date', 'Channel', 'Product family', 'Total', 'Orders']

    all_inova = all_inova.groupby(['Date', 'Channel', 'Product family'])[['Total', 'Orders']].sum().reset_index()

    # By product
    keep_products = ['EAGLE EYES', 'GREEN MARVEL', 'GREEN MARVEL LEGS', 'ROTAFLEX', 'SKOON', 'TERRACOAT BATERIA', 'XTENDER', 'XSHOCK', 'XSHOCK VORTEX', 'UROCAPS', 'SOGNARE ALMOHADA BASE']    
    df = df[df['Familia de Producto'].isin(keep_products)]

    # Change XSHOCK VORTEX to XSHOCK
    df['Familia de Producto'] = df['Familia de Producto'].replace('XSHOCK VORTEX', 'XSHOCK')
    keep_products.remove('XSHOCK VORTEX')

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

    new_columns = ['Date']
    for product_channel in orders_pivot.columns:
        new_columns.append(product_channel + ' - Orders')
        new_columns.append(product_channel + ' - Revenue')

    result = result[new_columns]
    
    return df, result

def processing_dks_inova_payment(raw_mow, raw_tkm, catalog):
    # Keep relevant columns
    keep_columns = ['Channel', 'Fecha', 'Familia de Producto', 'Orden', 'TPago']
    
    raw_mow = raw_mow[keep_columns]
    raw_tkm = raw_tkm[keep_columns]
    
    # Join dfs
    df = pd.concat([raw_mow, raw_tkm])
    
    # Create All Inova category
    temp_df = df[~df['Familia de Producto'].isin(['SOGNARE ALMOHADA BASE', 'EAGLE EYES'])]
    temp_df = df.groupby(['Fecha', 'Channel', 'TPago'])[['Orden']].count().reset_index()
    temp_df.columns = ['Date', 'Channel', 'Payment method', 'Count']
    
    all_inova_payment = pd.merge(temp_df, catalog[['ORIGEN DE VENTA', 'CANAL']], how = 'left', left_on = 'Channel', right_on = 'ORIGEN DE VENTA')
    all_inova_payment = all_inova_payment[['Date', 'CANAL', 'Payment method', 'Count']]
    all_inova_payment.columns = ['Date', 'Channel', 'Payment method', 'Count']
    all_inova_payment = all_inova_payment[all_inova_payment['Channel'].isin(['WEB ASISTIDA', 'WEB SELF SERVICES'])]
    
    all_inova_payment = all_inova_payment.groupby(['Date', 'Payment method'])[['Count']].sum().reset_index()
    pivot_df = all_inova_payment.pivot_table(index = 'Date', columns = 'Payment method', values = 'Count', fill_value = 0).reset_index()

    return pivot_df

def processing_dks_sognare(raw_df, catalog_product, catalog_channel, add_inova_products = None):
    # Keep relevant columns
    keep_columns = ['Channel', 'Status', 'Fecha', 'Total Products', 'Total Descuento', 'Familia de Producto', 'Orden']

    df = raw_df[keep_columns]
    df['Fecha'] = pd.to_datetime(df['Fecha'])

    # Filtering and processing
    df = df[(df['Status'] != 'Cancelled') & (df['Status'] != 'Void')]
    df['Total Order'] = df['Total Products'] - df['Total Descuento']

    # Add channel/product
    df = pd.merge(df, catalog_channel, on = 'Channel', how = 'left')
    df = pd.merge(df, catalog_product, on = 'Familia de Producto', how = 'left')

    # Add Inova MX and TKM products: Almohada base
    if add_inova_products:
        raw_mow = add_inova_products[0]
        raw_tkm = add_inova_products[1]
        mow_catalog = add_inova_products[2]

        # Keep relevant columns
        keep_columns = ['Channel', 'Status', 'Fecha', 'Total Products', 'Total Descuento', 'Familia de Producto', 'Orden']

        raw_mow = raw_mow[keep_columns]
        raw_tkm = raw_tkm[keep_columns]

        # Join dfs
        df_dks = pd.concat([raw_mow, raw_tkm])
        df_dks['Fecha'] = pd.to_datetime(df_dks['Fecha'])

        # Filtering and processing
        df_dks = df_dks[(df_dks['Status'] != 'Cancelled') & (df_dks['Status'] != 'Void')]
        df_dks['Total Order'] = df_dks['Total Products'] - df_dks['Total Descuento']
        mow_catalog['CANAL'] = mow_catalog['CANAL'].replace({'WEB SELF SERVICES': 'WEB SELF SERVICE'})

        # By product
        keep_products = ['SOGNARE ALMOHADA BASE']
        df_dks = df_dks[df_dks['Familia de Producto'].isin(keep_products)]

        df_dks = pd.merge(df_dks, mow_catalog[['ORIGEN DE VENTA', 'CANAL']], how = 'left', left_on = 'Channel', right_on = 'ORIGEN DE VENTA')
        df_dks = df_dks.drop(['ORIGEN DE VENTA'], axis = 1)
        df_dks['Product Category'] = 'ALMOHADA'

        df = pd.concat([df, df_dks])

    # Create All Sognare category
    all_sognare = df.groupby(['CANAL', 'Fecha']).agg(Total_Order = ('Total Order', 'sum'), Orders = ('Orden', 'size')).reset_index()
    all_sognare.columns = ['Channel', 'Date', 'Total', 'Orders']
    all_sognare['Product category'] = 'ALL SOGNARE'

    # Keep relevant columns
    df = df[['Fecha', 'Orden', 'CANAL', 'Product Category', 'Total Order']]
    df.columns = ['Date', 'Order', 'Channel', 'Product category', 'Total']

    df = df.groupby(['Date', 'Product category', 'Channel']).agg(Orders = ('Order', 'size'), Total = ('Total', 'sum')).reset_index()

    # Join All Sognare and specific products
    df = pd.concat([df, all_sognare]).reset_index().drop(['index'], axis = 1)

    # Define all possible products and channels
    all_days = df['Date'].unique()
    all_products = ['ALL SOGNARE'] + catalog_product['Product Category'].unique().tolist()
    if add_inova_products:
        all_channels = list(set(catalog_channel['CANAL'].unique().tolist() + mow_catalog['CANAL'].unique().tolist()))
    else:
        all_channels = catalog_channel['CANAL'].unique().tolist()

    # Create a MultiIndex DataFrame with all combinations of products and channels
    multi_index = pd.MultiIndex.from_product([all_days, all_products, all_channels], names = ['Date', 'Product category', 'Channel'])

    # Reindex the original DataFrame to this new index
    df_reindexed = df.set_index(['Date', 'Product category', 'Channel']).reindex(multi_index, fill_value = 0).reset_index()

    # Create a new column for the combination of Product family and Channel
    df_reindexed['Product_Channel'] = df_reindexed['Product category'] + ' - ' + df_reindexed['Channel']

    # Pivot the DataFrame for Orders
    orders_pivot = df_reindexed.pivot(index = 'Date', columns = 'Product_Channel', values = 'Orders')

    # Pivot the DataFrame for Revenue
    revenue_pivot = df_reindexed.pivot(index = 'Date', columns = 'Product_Channel', values = 'Total')

    # Combine Orders and Revenue into a single DataFrame
    result = pd.concat([orders_pivot.add_suffix(' - Orders'), revenue_pivot.add_suffix(' - Revenue')], axis = 1).reset_index()

    # Reorder
    new_columns = ['Date']
    for product_channel in orders_pivot.columns:
        new_columns.append(product_channel + ' - Orders')
        new_columns.append(product_channel + ' - Revenue')
    result = result[new_columns]
    result['Date'] = pd.to_datetime(result['Date'])

    # Reordenar columnas: mover ciertos productos/canales al final
    move_to_end = []
    keep_columns = ['Date']
    for col in result.columns:
        if col == 'Date':
            continue
        channel_match = 'SUPER SOFIA IA' in col
        product_match = any(p in col for p in ['BACTICURE', 'PULSERA FORTUNARA', 'SOGNARE COLCHON BIOFLEX'])
        if channel_match or product_match:
            move_to_end.append(col)
        else:
            keep_columns.append(col)

    # Agregar al final los que queremos mover
    final_columns = keep_columns + move_to_end
    result = result[final_columns]

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
