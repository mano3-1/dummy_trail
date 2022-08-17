import os,time
import dateutil.relativedelta
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
import time
from matching_name import *
from snowflake.connector.pandas_tools import write_pandas

from dateutil import parser
import numpy as np

import os
from cleaning_process_new import *
from datetime import datetime
import warnings
import sys
import tqdm
warnings.simplefilter(action='ignore', category=FutureWarning)
from cleaning_process_new import *

print("******* START *******")
logger_main.info("******* START *******")

input_folder = os.path.abspath(os.path.join(main_folder, 'input'))

for input in os.listdir(input_folder):
	print("\n=============================================================")
	logger_main.info("\n=============================================================")
	print("input: "+str(input))
	logger_main.info("input: "+str(input))
	deal_input_folder = os.path.abspath(os.path.join(input_folder, input))
	df_sales_map,df_inv_map = preprocessing(deal_input_folder)

	if not df_sales_map.empty and not df_inv_map.empty:
		print("call calculations code!!")
        
        ##COLUMN NAMES
        UNIQUE_DICT = {
            "COUNTRY" : "MAJOR_FRAME",
            "DIVISION" : "MINOR_FRAME",
            "INVENTORY_TYPE" : "SUB_MINOR_FRAME"
            
            }   #change made by manoj
        
        REV_UNIQUE_DICT = {
            'MAJOR_FRAME':'COUNTRY',
            'MINOR_FRAME':'DIVISION',
            'SUB_MINOR_FRAME':'INVENTORY_TYPE'
            }
        print("START")
        
        ctx = snowflake.connector.connect(
            user="USINGH",
            password="Dpa@12345",
            account="ch85105.us-east-1",
            warehouse="APPRAISAL_WH",
            database="TG_DB",
            schema="TEST_PYTHON_SCHEMA",
        )
        
        cur = ctx.cursor()
        print("CONNECTED:", cur)
        cur.execute("USE ROLE SELECT_ACCESS")
        
        
        final_start = time.time()
        total_insert_time = 0
        

        def renaming(df):
            df = df.rename({'COUNTRY':'MAJOR_FRAME','DIVISION':'MINOR_FRAME','INVENTORY_TYPE':'SUB_MINOR_FRAME'},axis=1)
            return df
        
        if not df_sales.empty and not df_inv.empty:
            df_key_mapping = pd.read_excel('Header mapping_1.xlsx')
        
            def initial_preprocess(df):
                #df['TRIM_SKU'] = df['TRIM_SKU'].astype(int, errors='coerce')
                df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
                return df
        
            def mapping_col(df, key_mapping):
                df = initial_preprocess(df)
                matched_df = match(df_key_mapping, 'Mapped Key', df.columns, 1)
                matched_df = matched_df[matched_df['distance'] == 0]
                print(matched_df[['Matched name_1','Original name']])
                key_mapping = key_mapping[df_key_mapping['Mapped Key'].isin(
                    matched_df['Matched name_1'])]
                key_mapping.loc[key_mapping['Mapped Key'].isin(
                    matched_df['Matched name_1']), 'Database']
                key_mapping = pd.merge(key_mapping, matched_df,
                                       left_on='Mapped Key', right_on='Matched name_1')
                key_mapping = key_mapping[~(key_mapping['Original name'].isin(
                    ['PROJ_DATE', 'PROJECT_AS_OF_DATE', 'EOM_DATE', 'BEG_MO', 'BOM_INV_DATE', 'EOM_INV_DATE']))]
                rename_dict = key_mapping[['Original name', 'Database']].set_index(
                    'Original name').to_dict()['Database']
                df = df.rename(rename_dict, axis=1)
        
                return df, key_mapping, matched_df
        
            df_sales_map, match_sale, temp1 = mapping_col(df_sales, df_key_mapping)
            df_sales_map.rename(
                columns={'PROJECT_AS_OF_DATE, PROJ_DATE': 'PROJ_DATE'}, inplace=True)
            df_inv_map, match_inv, temp2 = mapping_col(df_inv, df_key_mapping)
            df_inv_map.rename(
                columns={'PROJECT_AS_OF_DATE, PROJ_DATE': 'PROJECT_AS_OF_DATE'}, inplace=True)
        
            print("---------------insert in temp tables-----------------")
        
            #insertIntoTempTables(renaming(df_sales_map), 'TEMP_MO_SALES')
            #insertIntoTempTables(renaming(df_inv_map), 'TEMP_MO_INVENTORY')
        
            print("*******************************************")
        
            print("call calculations code")
           
        def fix_datatypes(df_sales, server_table):
            cols = list(df_sales.columns)
        
            for col in cols:
                k = server_table.loc[server_table.name == col, "type"].tolist()
                if len(k) == 0:
                    print("****!!warning!!****")
                    print(f"Encountered new column : {col}")
                    continue
                
                if k[0] == "int":
                    df_sales[col] = df_sales[col].astype(int)
                elif k[0] == "str":
                    df_sales[col]= df_sales[col].astype(str)
                elif k[0] == "float":
                    df_sales[col] = df_sales[col].astype(float)
                elif k[0] == "datetime":
                    df_sales[col] = pd.to_datetime(df_sales[col])
                else:
                    print(f"Unknow data type!! : {k[0]}")
            return df_sales
            
        server_table = pd.read_csv("python_temp_inv.csv")
        df_inv_map = fix_datatypes(df_inv_map,server_table)
        
        
        server_table = pd.read_csv("python_temp_sales.csv")
        df_sales_map = fix_datatypes(df_sales_map,server_table)
        
        print("""#####################################################################################
            
        "1. CREATION OF TEMP BUCKETS TABLES"
        
        #####################################################################################""")
        
        
        
        def mapping_col(df, key_mapping):
        
            df = initial_preprocess(df)
            matched_df = match(df_key_mapping, 'Mapped Key', df.columns, 1)
            matched_df = matched_df[matched_df['distance'] == 0]
            key_mapping = key_mapping[df_key_mapping['Mapped Key'].isin(
                matched_df['Matched name_1'])]
            key_mapping.loc[key_mapping['Mapped Key'].isin(
                matched_df['Matched name_1']), 'Database']
            key_mapping = pd.merge(key_mapping, matched_df,
                                   left_on='Mapped Key', right_on='Matched name_1')
            key_mapping = key_mapping[~(key_mapping['Original name'].isin(
                ['PROJ_DATE', 'PROJECT_AS_OF_DATE', 'EOM_DATE', 'BEG_MO', 'BOM_INV_DATE', 'EOM_INV_DATE']))]
            rename_dict = key_mapping[['Original name', 'Database']].set_index(
                'Original name').to_dict()['Database']
            df = df.rename(rename_dict, axis=1)
        
            return df, key_mapping, matched_df
        
        
        def intesection(l1, l2):
            return list(set(l1) & set(l2))
        
        
        def preprocess_before_insertion(df, len_dim_cat):
        
            df[df.filter(regex='DESC').columns] = df.filter(
                regex='DESC').fillna('NEED CAT')
            df[df.filter(regex='CODE').columns] = df.filter(regex='CODE').fillna('NA')
            df['INSERT_DATE'] = datetime.now().date()
            if len_dim_cat == 0:
                df.drop('CATEGORY_SEQ_ID', axis=1, inplace=True)
            else:
                df = df[df['CATEGORY_SEQ_ID'].isna()]
                df.drop('CATEGORY_SEQ_ID', axis=1, inplace=True)
        
            return df
        
        # def renaming(df):
        #     df = df.rename({'COUNTRY':'MAJOR_FRAME','DIVISION':'MINOR_FRAME','INVENTORY_TYPE':'SUB_MINOR_FRAME'},axis=1)
        #     return df
        # def mapping():
        
        
        #df_sales = pd.read_excel('input/!TVS_JJ_Monthly Sales.xlsx')
        df_sales_map = df_sales_map.rename({'BOM': 'BEG_MO', 'EOM': 'EOM_DATE'}, axis=1)
        #df_sales = df_sales.rename({'10010 - Project As of Date': 'PROJ_DATE'}, axis=1)
        #df_inv = pd.read_excel('input/!TVS_JJ_Monthly Inventory.xlsx')
        df_inv_map = df_inv_map.rename({'BOM': 'BOM_INV_DATE', 'EOM': 'EOM_INV_DATE'}, axis=1)
        ###11/08
        df_inv_map['SKU']=df_inv_map['SKU'].astype(str)
        df_inv_map['TRIM_SKU']=df_inv_map['TRIM_SKU'].astype(str)
        df_sales_map['SKU']=df_sales_map['SKU'].astype(str)
        df_sales_map['TRIM_SKU']=df_sales_map['TRIM_SKU'].astype(str)
        ###
        #df_inv = df_inv.rename(
        #    {'10010 - Project As of Date': 'PROJECT_AS_OF_DATE'}, axis=1)
        #
        #df_sales = df_sales.rename({'TRIMSKU': 'TRIM_SKU'}, axis=1)
        #df_inv = df_inv.rename({'TRIMSKU': 'TRIM_SKU'}, axis=1)
        #
        #df_key_mapping = pd.read_excel('mapping.xlsx')
        #
        #df_sales_map, match_sale, temp1 = mapping_col(df_sales, df_key_mapping)
        #df_inv_map, match_inv, temp2 = mapping_col(df_inv, df_key_mapping)
        #
        #deal_id = df_sales_map.loc[0, 'DEAL_ID']
        #
        #df_sales_map = initial_preprocess(df_sales_map)
        #df_inv_map = initial_preprocess(df_inv_map)
        
        #    return df_inv_map, df_sales_map, deal_id
        
        print("""#########################################################
        
        "TEMP BUCKET"
        
        #########################################################""")
        
        df_sales_map.columns = [i.upper() for i in df_sales_map.columns]
        df_inv_map.columns = [i.upper() for i in df_inv_map.columns]
        df_inv_map.columns
        # df_inv_map.dtypes
        # df_inv_map = renaming(df_inv_map)
        # df_sales_map = renaming(df_sales_map)
        
        deal_id = df_inv_map.loc[0,'DEAL_ID']
        
        from datetime import datetime
        
        def get_introduction_date(row):
            '''
            This function is for computing introduction date using the following logic:
                1. If both are 1-1-1900, return 1-1-1900
                2. If one of them is 1-1-1900, return non 1-1-1900
                3. If both are not 1-1-1900, return the least one
            args are self explanatory
            '''
            FIRST_SALE_DATE, FIRST_RECEIPT_DATE = row['FIRST_SALE_DATE'], row['FIRST_RECEIPT_DATE']
            zero_date = datetime(1900, 1, 1).date()
            if (FIRST_SALE_DATE == zero_date and FIRST_RECEIPT_DATE == zero_date):
                return zero_date
            elif(FIRST_SALE_DATE != zero_date and FIRST_RECEIPT_DATE == zero_date):
                return FIRST_SALE_DATE
            elif(FIRST_SALE_DATE == zero_date and FIRST_RECEIPT_DATE != zero_date):
                return FIRST_RECEIPT_DATE
            else:
                return min(FIRST_RECEIPT_DATE, FIRST_SALE_DATE)
        
        def temp_bucket(df, flag, deal_id, flag_fact=True):
            """
            
            """
            temp_bucket = df.copy()
            comb_bucket = intesection(temp_bucket.columns, df)
            temp_bucket[comb_bucket] = df[comb_bucket].copy()
            
            temp_bucket['INTRODUCTION_DATE'] = temp_bucket[[
                'FIRST_SALE_DATE', 'FIRST_RECEIPT_DATE']].apply(get_introduction_date, axis = 1)
        
            temp_bucket.loc[temp_bucket['INTRODUCTION_DATE'].isna(
            ), 'INTRODUCTION_DATE'] = temp_bucket['FIRST_RECEIPT_DATE']
        #    print (temp_bucket.columns)
            if flag == 'inv':
                temp_bucket['LAST_SALES_DAYS'] = (pd.to_datetime(
                    temp_bucket['BOM_INV_DATE'])-pd.to_datetime(temp_bucket['LAST_SALES_DATE'])).dt.days
                temp_bucket['FIRST_SALES_DAYS'] = (pd.to_datetime(
                    temp_bucket['BOM_INV_DATE'])-pd.to_datetime(temp_bucket['FIRST_SALE_DATE'])).dt.days
                temp_bucket['INTRODUCTION_DAYS'] = (pd.to_datetime(
                    temp_bucket['BOM_INV_DATE'])-pd.to_datetime(temp_bucket['INTRODUCTION_DATE'])).dt.days
                temp_bucket['LAST_RECEIPT_DAYS'] = (pd.to_datetime(
                    temp_bucket['BOM_INV_DATE'])-pd.to_datetime(temp_bucket['LAST_RECEIPT_DATE'])).dt.days
                temp_bucket['FIRST_RECEIPT_DAYS'] = (pd.to_datetime(
                    temp_bucket['BOM_INV_DATE'])-pd.to_datetime(temp_bucket['FIRST_RECEIPT_DATE'])).dt.days
                                
            elif flag == 'sale':
                temp_bucket['LAST_SALES_DAYS'] = (pd.to_datetime(
                    temp_bucket['BEG_MO'])-pd.to_datetime(temp_bucket['LAST_SALES_DATE'])).dt.days
                temp_bucket['FIRST_SALES_DAYS'] = (pd.to_datetime(
                    temp_bucket['BEG_MO'])-pd.to_datetime(temp_bucket['FIRST_SALE_DATE'])).dt.days
                temp_bucket['INTRODUCTION_DAYS'] = (pd.to_datetime(
                    temp_bucket['BEG_MO'])-pd.to_datetime(temp_bucket['INTRODUCTION_DATE'])).dt.days
                temp_bucket['LAST_RECEIPT_DAYS'] = (pd.to_datetime(
                    temp_bucket['BEG_MO'])-pd.to_datetime(temp_bucket['LAST_RECEIPT_DATE'])).dt.days
                temp_bucket['FIRST_RECEIPT_DAYS'] = (pd.to_datetime(
                    temp_bucket['BEG_MO'])-pd.to_datetime(temp_bucket['FIRST_RECEIPT_DATE'])).dt.days
                        
            date_key = pd.read_excel('Buckets_Range.xlsx')
        
            def buckets(bucket, col1, col2):
                for i in range(len(date_key)):
                    lb = date_key.loc[i, 'Lower Bound']
                    ub = date_key.loc[i, 'Upper Bound']
        
                    bucket.loc[bucket[col1].between(
                        lb, ub), col2] = date_key.loc[i, 'Value']
                return bucket
        
            if flag_fact:
                temp_bucket = buckets(
                    temp_bucket, 'INTRODUCTION_DAYS', 'INTRODUCTION_BUCKET')
                temp_bucket = buckets(
                    temp_bucket, 'FIRST_SALES_DAYS', 'FIRST_SALE_DATE_BUCKET')
                temp_bucket = buckets(
                    temp_bucket, 'LAST_SALES_DAYS', 'LAST_SALE_DATE_BUCKET')
                temp_bucket = buckets(
                    temp_bucket, 'LAST_RECEIPT_DAYS', 'LAST_RECEIPT_DATE_BUCKET')
                temp_bucket = buckets(
                    temp_bucket, 'FIRST_RECEIPT_DAYS', 'FIRST_RECEIPT_DATE_BUCKET')
        
            return temp_bucket
        
        
        def bucket_creation(df, comb_bkt, comb_bkt1, agg_col, bom_col, first_date, last_date, deal_id):
        
            agg_dict = {}
            for i in agg_col:
                agg_dict[i] = 'sum'
            
            temp_bkt = df.groupby(comb_bkt)[agg_col].sum().reset_index()
            temp_bkt_not_null = temp_bkt[(temp_bkt[agg_col[0]] != 0)]  ####change 0 to 1 ie qty to cost for inv
            temp_bkt1 = temp_bkt_not_null.groupby(
                comb_bkt1).agg({bom_col: 'min'}).reset_index()#first sale/receipt date
            temp_bkt2 = temp_bkt_not_null.groupby(
                comb_bkt1).agg({bom_col: 'max'}).reset_index()# for last/receipt sale date
            temp_bkt1 = temp_bkt1.rename({bom_col: first_date}, axis=1)
            temp_bkt2 = temp_bkt2.rename({bom_col: last_date}, axis=1)
        
            req_df = pd.merge(temp_bkt1, temp_bkt, on=comb_bkt1, how='right')
            req_df = pd.merge(temp_bkt2, req_df, on=comb_bkt1, how='right')
        
            req_df.loc[req_df[first_date].isna(), first_date] = datetime(
                1900, 1, 1).date()
            req_df.loc[req_df[last_date].isna(), last_date] = datetime(
                1900, 1, 1).date()
        
            req_df['INSERT_DATE'] = datetime.now().date()
            req_df.drop(agg_col, axis=1, inplace=True)
        
            req_df['DEAL_ID'] = deal_id
        
            return req_df
        
        
        def merge_date_insertion(sale_df, inv_df, comb_bkt1, deal_id):
            
            insert_time = 0
            merge_date = pd.merge(sale_df[comb_bkt1+['FIRST_SALE_DATE', 'LAST_SALES_DATE']], inv_df[comb_sale_bkt1+[
                                  'FIRST_RECEIPT_DATE', 'LAST_RECEIPT_DATE']], on=comb_bkt1, how='right').drop_duplicates()
            merge_date.index = range(len(merge_date))
        
            inv_df = pd.merge(
                inv_df, merge_date[comb_bkt1+['FIRST_SALE_DATE', 'LAST_SALES_DATE']], on=comb_bkt1, how='left')
            sale_df = pd.merge(
                sale_df, merge_date[comb_bkt1+['FIRST_RECEIPT_DATE', 'LAST_RECEIPT_DATE']], on=comb_bkt1, how='left')
        
            inv_df.loc[inv_df['FIRST_SALE_DATE'].isna(
            ), 'FIRST_SALE_DATE'] = datetime(1900, 1, 1).date()
            inv_df.loc[inv_df['LAST_SALES_DATE'].isna(
            ), 'LAST_SALES_DATE'] = datetime(1900, 1, 1).date()
        
            sale_df.loc[sale_df['FIRST_RECEIPT_DATE'].isna(
            ), 'FIRST_RECEIPT_DATE'] = datetime(1900, 1, 1).date()    
            sale_df.loc[sale_df['LAST_RECEIPT_DATE'].isna(
            ), 'LAST_RECEIPT_DATE'] = datetime(1900, 1, 1).date()
        
            sale_bucket = temp_bucket(sale_df, 'sale', deal_id)
            inv_bucket = temp_bucket(inv_df, 'inv', deal_id)
            
        #    if len(inv_bucket) != 0:
            inv_bucket = inv_bucket.applymap(
                lambda x: x.date() if isinstance(x, datetime) else x)
            db_table = pd.read_excel(
                'Test Python Schema Tables (2).xlsx', sheet_name='TEMP_INVBUCKET')
            db_inv_bucket = pd.DataFrame(
                columns=db_table.iloc[:,1].tolist(), dtype=object)
            db_inv_bucket[intesection(db_table.iloc[:,1].tolist(), inv_bucket)] = inv_bucket[intesection(db_table.iloc[:,1].tolist(), inv_bucket)]
        #        print ()
            db_inv_bucket[[i for i in db_inv_bucket.columns if 'DATE' in i]] = db_inv_bucket[[i for i in db_inv_bucket.columns if 'DATE' in i]].astype(str)
        #        db_inv_bucket.to_excel('inv_bucket.xlsx',index=False)
            s1 = time.time()
           
            print (2)
            print(db_inv_bucket.columns)
            #success1, nchunks, nrows, _ = write_pandas(ctx, renaming(db_inv_bucket), 'TEMP_INVBUCKET')
            s2 = time.time()
            insert_time = insert_time + (s2-s1)
        
        #    elif len(sale_bucket) != 0:
            sale_bucket = sale_bucket.applymap(
                lambda x: x.date() if isinstance(x, datetime) else x)
            db_table = pd.read_excel(
                'Test Python Schema Tables (2).xlsx', sheet_name='TEMP_SALESBUCKET')
            db_sale_bucket = pd.DataFrame(
                columns=db_table.iloc[:,1].tolist(), dtype=object)
            db_sale_bucket[intesection(db_table.iloc[:,1].tolist(), sale_bucket)] = sale_bucket[intesection(db_table.iloc[:,1].tolist(), sale_bucket)]
            db_sale_bucket[[i for i in db_sale_bucket.columns if 'DATE' in i]] = db_sale_bucket[[i for i in db_sale_bucket.columns if 'DATE' in i]].astype(str)
        #        db_sale_bucket.to_excel('sale_bucket.xlsx',index=False)
            s1 = time.time()
            print(db_sale_bucket.columns)
            
            #success2, nchunks, nrows, _ = write_pandas(ctx, renaming(db_sale_bucket), 'TEMP_SALESBUCKET')
            
            s2 = time.time()
            insert_time = insert_time + (s2-s1)
                
            return sale_bucket, inv_bucket, insert_time
        
        
        comb_bucket = ['BOM_INV_DATE', 'COUNTRY','DIVISION','LOCATION',
                       'INVENTORY_TYPE', 'TRIM_SKU', 'BEG_MO']
        
        comb_inv_bkt = intesection(comb_bucket, df_inv_map.columns)
        comb_sale_bkt = intesection(comb_bucket, df_sales_map.columns)
        
        comb_inv_bkt1 = comb_inv_bkt.copy()
        comb_inv_bkt1.remove('BOM_INV_DATE')
        
        comb_sale_bkt1 = comb_sale_bkt.copy()
        comb_sale_bkt1.remove('BEG_MO')
        
        #df_inv_map, df_sales_map, deal_id = mapping()
        #df, comb_bkt, comb_bkt1, agg_col, bom_col, first_date, last_date, deal_id
        df_inv_map.groupby(['BOM_INV_DATE'])[['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED']].sum() 
        df_inv_map.columns
        inv = bucket_creation(df_inv_map, comb_inv_bkt, comb_inv_bkt1, ['TOTAL_COST_EXTENDED','QUANTITY_ON_HAND'], 'BOM_INV_DATE', 'FIRST_RECEIPT_DATE', 'LAST_RECEIPT_DATE', deal_id)  ##using total cost extended for receipt date
        sale = bucket_creation(df_sales_map, comb_sale_bkt, comb_sale_bkt1, intesection(df_sales_map.columns, [
                               'QUANTITY_SOLD', 'NET_SALES_$', 'GROSS_MARGIN_$', 'COGS_$']), 'BEG_MO', 'FIRST_SALE_DATE', 'LAST_SALES_DATE', deal_id)
        
        # inv  --- first receipt date, last receipt date
        # sale --- fist sale date, last sale date
        
        if set(comb_inv_bkt1) == set(comb_sale_bkt1):
        
            sale_bkt, inv_bkt, insert_time = merge_date_insertion(sale, inv, comb_inv_bkt1, deal_id)
        
            total_insert_time = total_insert_time + insert_time
            print(sale_bkt, '-----', inv_bkt)
            print('/n/n----------------DONE with TEMP BUCKETS----------------------')
            print('\n\n\n\n')
        
        else:
            print('################# PLEASE CHECK RAW FILES ####################')
            print('\n\n\n\n')
        
        
        try:
            df_inv_map.LOT = df_inv_map.LOT.astype(str)
        except:
            pass
        
        try:
            df_sales_map.LOT = df_sales_map.LOT.astype(str)
        except:
            pass
        
        print("""##########################################################################################
        
        '2. DIM_CATGORY'
        
        ##########################################################################################""")
        
        db_table = pd.read_excel(
            'Test Python Schema Tables (2).xlsx', sheet_name='DIM_CATEGORY')
        cols = db_table.iloc[:,1].tolist()
        
        db_dim_cat = pd.DataFrame(columns=cols, dtype=object)
        
        try:
            cur.execute(
                f"SELECT CATEGORY_SEQ_ID, DEAL_ID,CATEGORY_1,CATEGORY_2,CATEGORY_3,CATEGORY_4,CATEGORY_CODE_1,CATEGORY_CODE_2,CATEGORY_CODE_3,CATEGORY_CODE_4,SKU,COUNTRY,DIVISION,LOCATION,INVENTORY_TYPE from DIM_CATEGORY where DEAL_ID = {deal_id}")
            dim_cat = cur.fetch_pandas_all()
            sys.exit('############ PLEASE DELETE DATA FROM DB ###################')
        except:
            dim_cat = pd.DataFrame(columns=['CATEGORY_SEQ_ID', 'DEAL_ID', 'CATEGORY_1', 'CATEGORY_2', 'CATEGORY_3', 'CATEGORY_4', 'CATEGORY_CODE_1',
                                   'CATEGORY_CODE_2', 'CATEGORY_CODE_3', 'CATEGORY_CODE_4', 'SKU', 'LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE'], dtype=object)
        
        comb_to_match_sale = intesection(
            dim_cat.columns.tolist(), df_sales_map.columns.tolist())
        comb_to_match_inv = intesection(
            dim_cat.columns.tolist(), df_inv_map.columns.tolist())
        
        inv_prior = ['DEAL_ID', 'SKU', 'LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE']
        inv_prior = intesection(inv_prior, comb_to_match_inv)
        
        if len(comb_to_match_inv) < len(comb_to_match_sale):
            missing = list(sorted(set(comb_to_match_sale) - set(comb_to_match_inv)))
            for i in missing:
                df_inv_map[i] = np.nan
        
        elif len(comb_to_match_inv) > len(comb_to_match_sale):
        
            missing = list(sorted(set(comb_to_match_inv) - set(comb_to_match_sale)))
            for i in missing:
                df_sales_map[i] = np.nan
        
        df_sale_map_grp = df_sales_map.groupby(comb_to_match_sale).agg(
            {'INVOICE_DATE': 'max'}).reset_index()
        df_inv_map_grp = df_inv_map.groupby(comb_to_match_inv).agg(
            {'INVENTORY_DATE': 'max'}).reset_index() # groupby needs agg column
        
        combined_dim = df_inv_map[comb_to_match_inv].append(
            df_sales_map[comb_to_match_sale]).fillna("NEED CAT").drop_duplicates(inv_prior, keep='first')
        
        df_sales_map[comb_to_match_sale]
        
        if len(comb_to_match_inv)<len(comb_to_match_sale):    #change made by manoj
            UNIQUE_COMB = comb_to_match_sale.copy() #change made by manoj
        else: #change made by manoj
                UNIQUE_COMB = comb_to_match_inv.copy()#change made by manoj
                
        dim_cat[UNIQUE_COMB] = combined_dim[UNIQUE_COMB].copy()#change made by manoj
        dim_cat = preprocess_before_insertion(dim_cat, len(dim_cat))
        
        
        db_dim_cat[UNIQUE_COMB] = dim_cat[UNIQUE_COMB].copy()#change made by manoj
        db_dim_cat = preprocess_before_insertion(db_dim_cat, len(db_dim_cat))
        # db_dim_cat['CATEGORY_SEQ_ID'] = np.random.randint(
        #     100000, 100000000, len(db_dim_cat))        #change made by manoj
        db_dim_cat[["CATEGORY_1", "CATEGORY_2", "CATEGORY_3", "CATEGORY_4"]] = db_dim_cat[["CATEGORY_1", "CATEGORY_2", "CATEGORY_3", "CATEGORY_4"]].fillna("NEED CAT") 
        #db_dim_cat['DEPT_DESC'] = db_dim_cat['DEPT_DESC'].astype(str)
        #db_dim_cat['CLASS_CODE'] = db_dim_cat['CLASS_CODE'].astype(str)
        db_dim_cat['SKU'] = db_dim_cat['SKU'].astype(str)
        s1 = time.time()
        #success, nchunks, nrows, _ = write_pandas(ctx, renaming(db_dim_cat), 'DIM_CATEGORY',parallel=99, chunk_size=50000)
        s2 = time.time()
        
        
        total_insert_time = total_insert_time + (s2-s1)
        
        print('######## DONE WITH DIM CATEGORY #########\n\n\n\n\n')
        
        print('''#######################################################
                
        "3. DIM_SKU"
        
        #########################################################''')
        
        db_table = pd.read_excel(
            'Test Python Schema Tables (2).xlsx', sheet_name='DIM_SKU')
        cur.execute(
            f'SELECT CATEGORY_SEQ_ID, DEAL_ID,CATEGORY_1,CATEGORY_2,CATEGORY_3,CATEGORY_4,CATEGORY_CODE_1,CATEGORY_CODE_2,CATEGORY_CODE_3,CATEGORY_CODE_4,SKU,{UNIQUE_DICT["COUNTRY"]},{UNIQUE_DICT["DIVISION"]},LOCATION,{UNIQUE_DICT["INVENTORY_TYPE"]} from DIM_CATEGORY where DEAL_ID = {deal_id}') #change made by manoj
        db_dim_cat = cur.fetch_pandas_all() #change made by manoj
        db_dim_cat = db_dim_cat.rename(REV_UNIQUE_DICT, axis=1) #change made by manoj
        
        try:
            cur.execute(
                f"SELECT SKU_SEQ_ID,CATEGORY_SEQ_ID, DEAL_ID,SKU,LOCATION, COUNTRY, DIVISION,INVENTORY_TYPE from DIM_SKU where DEAL_ID = {deal_id}")
            dim_sku = cur.fetch_pandas_all()
            if len(dim_sku) > 0:
                sys.exit('############ PLEASE DELETE DATA FROM DB ###################')
        except:
            dim_sku = pd.DataFrame(columns=['SKU_SEQ_ID', 'CATEGORY_SEQ_ID', 'DEAL_ID',
                                   'SKU', 'LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE'], dtype=object)
        
        
        db_dim_sku = pd.DataFrame(columns=db_table['name'].tolist(), dtype=object)
        
        dim_sku_sale = intesection(db_dim_sku.columns.tolist(),
                                   df_sales_map.columns.tolist())
        dim_sku_inv = intesection(db_dim_sku.columns.tolist(),
                                  df_inv_map.columns.tolist())
        dim_sku_cols = dim_sku_inv + dim_sku_sale
        dim_sku_cols = list(set(dim_sku_cols))
        
        comb_to_match_sale_sku = intesection(
            dim_sku.columns.tolist(), df_sales_map.columns.tolist())
        comb_to_match_inv_sku = intesection(
            dim_sku.columns.tolist(), df_inv_map.columns.tolist())
        
        if len(comb_to_match_inv_sku) < len(comb_to_match_sale_sku):
        
            missing = list(sorted(set(comb_to_match_sale_sku) -
                           set(comb_to_match_inv_sku)))
            for i in missing:
                df_inv_map[i] = np.nan
        
        elif len(comb_to_match_inv_sku) > len(comb_to_match_sale_sku):
        
            missing = list(sorted(set(comb_to_match_inv_sku) -
                           set(comb_to_match_sale_sku)))
            for i in missing:
                df_sales_map[i] = np.nan
        
        db_dim_sku = pd.merge(dim_sku[comb_to_match_sale_sku], db_dim_cat[comb_to_match_sale_sku+[
                              'CATEGORY_SEQ_ID']], on=comb_to_match_sale_sku, how='right')
        db_dim_sku.drop_duplicates(comb_to_match_sale_sku, inplace=True)# can be replaced with groupby function
        db_dim_sku['INSERT_DATE'] = datetime.now().date()
        # db_dim_sku['SKU_SEQ_ID'] = np.random.randint(
        #     1000000000, 100000000000, len(db_dim_sku), dtype=np.int64)  #change made by manoj
        
        #t = df_sales_map[dim_sku_sale]
        
        db_dim_sku = pd.merge(df_inv_map[dim_sku_inv], db_dim_sku, on=comb_to_match_inv_sku,
                              how='right').drop_duplicates(comb_to_match_inv_sku)
        
        db_dim_sku = pd.merge(df_sales_map[dim_sku_sale], db_dim_sku,
                              on=comb_to_match_sale_sku, how='right').drop_duplicates(comb_to_match_sale_sku)
        
        db_dim_sku['TRIM_SKU'] = db_dim_sku['SKU'].str.strip()
        
        l1 = []
        l2 = []
        for i in db_dim_sku.columns:
            if ('_x' in i):
                l1.append(i)
                l2.append(i.replace('_x','_y'))
        for i in range(len(l1)):
            db_dim_sku.loc[db_dim_sku[l1[i]].isna(),l1[i]]=db_dim_sku[l2[i]]
            db_dim_sku.loc[db_dim_sku[l2[i]].isna(),l2[i]]=db_dim_sku[l1[i]]
        db_dim_sku.drop(l1, axis=1, inplace=True)
        
        db_dim_sku = db_dim_sku.rename(
            dict(zip(l2, [i.replace('_y', '') for i in l2])), axis=1)
        db_dim_sku.dtypes
        if 'DESCRIPTION_1' in db_dim_sku.columns:  ##11/08
            db_dim_sku['DESCRIPTION_1']=db_dim_sku['DESCRIPTION_1'].astype(str)
        s1 = time.time()
        
        try:
            db_dim_sku.LOT = db_dim_sku.LOT.astype(str)
        except:
            pass
        
        
        #success, nchunks, nrows, _ = write_pandas(ctx, renaming(db_dim_sku), 'DIM_SKU', parallel=99) # check if all the columns are uploaded or not 
        s2 = time.time()
        total_insert_time = total_insert_time + (s2-s1)
        
        cur.execute(
                f'SELECT SKU_SEQ_ID,CATEGORY_SEQ_ID, DEAL_ID,SKU,LOCATION,{UNIQUE_DICT["COUNTRY"]},{UNIQUE_DICT["DIVISION"]},{UNIQUE_DICT["INVENTORY_TYPE"]} from DIM_SKU where DEAL_ID = {deal_id}')
        db_dim_sku = cur.fetch_pandas_all()
        db_dim_sku.rename(REV_UNIQUE_DICT, axis=1, inplace = True)
        print('######## DONE WITH DIM SKU #########\n\n\n\n\n')
        
        db_dim_sku.isna().sum()
        
        #temp = df_inv_map[df_inv_map['SKU']=='2-ZIRAH095']
        print("""#######################################################
        
        "4. FACT SALES and INVENTORY"
        
        #######################################################""")
        
        
        
        def fact_creation(df, temp, comb_to_match, cols_db, agg_cols, bom_col, table_name, sku_df, flag):
            # df=df_inv_map.copy()
            # temp=inv_bkt.copy()
            # comb_to_match=comb_to_match_inv
            # cols_db=db_table_inv.iloc[:,1].tolist()
            # agg_cols=['QUANTITY_ON_HAND', 'TOTAL_COST_EXTENDED']
            # bom_col='BOM_INV_DATE'
            # table_name='FACT_INVENTORY'
            # sku_df=db_dim_sku.copy()
            # flag='inv'
            insert_time = 0
            
            agg_df = df.groupby(comb_to_match)[agg_cols].sum().reset_index()
            fact = pd.merge(df, agg_df, on=comb_to_match,
                            how='right').drop_duplicates(comb_to_match)
        
            rename_dict = {}
            for i in agg_cols:
                rename_dict[i+'_y'] = i
            rename_dict['DEAL_ID_y'] = 'DEAL_ID'
            fact = fact.rename(rename_dict, axis=1)
            fact.drop([i+'_x' for i in agg_cols], axis=1, inplace=True)
            fact = fact.applymap(lambda x: x.date() if isinstance(x, datetime) else x)
            temp = temp.applymap(lambda x: x.date() if isinstance(x, datetime) else x)
            fact = pd.merge(fact, temp, on=comb_to_match+['DEAL_ID'], how='left')
         
            # DIM SKU
            db_dim_sku['TRIM_SKU'] = db_dim_sku['SKU'].str.strip()
            if flag == 'inv':
                comb_to_match.remove('BOM_INV_DATE')
            else:
                comb_to_match.remove('BEG_MO')
            fact['TRIM_SKU']=fact['TRIM_SKU'].astype(str) ###10/08
            fact['SKU']=fact['SKU'].astype(str)  ###10/08
        
            fact = pd.merge(
                fact, db_dim_sku[['SKU_SEQ_ID']+comb_to_match], on=comb_to_match, how='left')
            
            # error=fact[fact['SKU_SEQ_ID'].isna()]
            
            # sku_err=list(set(fact['SKU'].unique())-set(db_dim_sku['SKU'].unique()))
            # db_dim_sku[db_dim_sku['SKU'].isin(sku_err)]
            # db_dim_sku[db_dim_sku['TRIM_SKU']=='13365']
            # fact[fact['TRIM_SKU']==13365]
            # fact.isna().sum()
            if flag == 'inv':
                fact = temp_bucket(fact, 'inv', deal_id)
            else:
                fact = temp_bucket(fact, 'sale', deal_id)
        #    fact_inv = fact_inv.rename({'FIRST_RECEIPT_DATE':'FIRSTRECEIPT_DATE'},axis=1)
        
        #     one_time_df = pd.read_csv('DIM_DATE.csv')
        # #    one_time_df = cur.fetch_pandas_all()
        #     one_time_df = one_time_df.rename({'DATE': bom_col}, axis=1)
        # #    print (one_time_df)
        #     fact = pd.merge(
        #         fact, one_time_df[[bom_col, 'DATE_KEY']], on=bom_col, how='left')
        
            fact["DATE_KEY"] = fact[bom_col].astype(str).str.replace('-','')   #change made by manoj
        
            l1 = []
            l2 = []
            for i in fact.columns:
                if ('_x' in i):
                    l1.append(i)
                elif ('_y' in i):
                    l2.append(i)
            fact.drop(l2, axis=1, inplace=True)
            fact.columns = [i.replace('_x', '') for i in fact.columns]
        
            
            if flag=='inv':
                fact['INVENTORY_SEQ_ID'] = np.random.randint(
                1000000000000, 10000000000000, len(fact), dtype=np.int64)
            else:
                fact['SALES_SEQ_ID'] = np.random.randint(
                1000000000000, 10000000000000, len(fact), dtype=np.int64)
            
            fact['INSERT_DATE'] = datetime.now().date()
            
            fact = fact.loc[:, ~fact.columns.duplicated()]
            
            fact[[i for i in fact.columns if 'DATE' in i]] = fact[[i for i in fact.columns if 'DATE' in i]].astype(str)
        #    print (db_fact['INTRODUCTION_BUCKET_SEQ_ID'])
            s1 = time.time()
            cols_to_insert = intesection(cols_db, fact.columns.tolist()) # taking intersection from db cols and raw columns
           
            fact_cpy = fact.copy()
           
            
            if flag == "inv":    
                fact_cpy["GROSS_INVENTORY_QUANTITY"] = fact_cpy["QUANTITY_ON_HAND"].copy()    #change made by manoj
                fact_cpy["GROSS_INVENTORY_COST"] = fact_cpy["TOTAL_COST_EXTENDED"].copy()    #change made by manoj
                fact_cpy["UNIT_COST"] = fact_cpy["GROSS_INVENTORY_COST"]/fact_cpy["GROSS_INVENTORY_QUANTITY"]  #change made by manoj 
        
                fact_cpy.loc[fact_cpy["GROSS_INVENTORY_QUANTITY"]==0, "UNIT_COST"] = 0
                cols_to_insert = cols_to_insert + ["GROSS_INVENTORY_QUANTITY", "GROSS_INVENTORY_COST", "UNIT_COST"]  #change made by manoj
            
            
            print(fact_cpy.dtypes)
            #success, nchunks, nrows, _ = write_pandas(ctx, renaming(fact_cpy[cols_to_insert]), table_name, parallel=99)   #change made by manoj
            s2 = time.time()
            insert_time = insert_time + (s2-s1)
            return fact, insert_time
        
        
        cols_to_groupby = ['BOM_INV_DATE', 'EXPIRATION_DATE', 'ORDER_DATE', 'SHIP_DATE',
                           'LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE', 'TRIM_SKU', 'BEG_MO']
        
        comb_to_match_sale = intesection(
            df_sales_map.columns.tolist(), cols_to_groupby)
        comb_to_match_inv = intesection(df_inv_map.columns.tolist(), cols_to_groupby)
        
        db_table_inv = pd.read_excel(
            'Test Python Schema Tables (2).xlsx', sheet_name='FACT_INVENTORY')
        
        db_table_sale = pd.read_excel('Test Python Schema Tables (2).xlsx', sheet_name='FACT_SALES')
        
        #cols_to_insert_inv = intesection(
        #    db_table_inv.iloc[:,1].tolist(), df_inv_map.columns.tolist())
        #cols_to_insert_sale = intesection(
        #    db_table_sale.iloc[:,1].tolist(), df_sales_map.columns.tolist())
        
        #start = time.time()
        df_sales_map.columns
        fact_inv_db, insert_time1 = fact_creation(df_inv_map, inv_bkt, comb_to_match_inv, db_table_inv.iloc[:,1].tolist(), ['QUANTITY_ON_HAND', 'TOTAL_COST_EXTENDED'], 'BOM_INV_DATE', 'FACT_INVENTORY', db_dim_sku, 'inv')
        
        #fact_inv_db['FIRST_SALE_DATE']
        fact_sale_db, insert_time2 = fact_creation(df_sales_map, sale_bkt, comb_to_match_sale, db_table_sale.iloc[:,1].tolist(), ['QUANTITY_SOLD', 'NET_SALES_$', 'GROSS_MARGIN_$', 'COGS_$'], 'BEG_MO', 'FACT_SALES', db_dim_sku, 'sale')
        
        
        # fact_inv_db.isna().sum()
        # db_dim_sku[db_dim_sku['SKU']=='13365']
        # sku_seq_na=fact_inv_db[fact_inv_db['SKU_SEQ_ID'].isna()]['SKU'].unique()
        
        # fact_inv_db[fact_inv_db['SKU'].isin(sku_seq_na)]['SKU_SEQ_ID'].unique()
        # fact.isna().sum()
        #end = time.time()
        total_insert_time = total_insert_time + (insert_time1+insert_time2)
        
        print('######## DONE WITH FACT SALES and INVENTORY #########\n\n\n\n\n')
        
        
        print('''################################################################################
               
        '5. DEMAND CREATION'
         
        #######################################################################################''')
        
        def demand_calculation(df, flag, month, bom_list, zero_cols):
        
            df['SALES_INFO_GM_AMOUNT'] = df['SALES_INFO_SOLD_AMOUNT'] - df['SALES_INFO_COGS']
            df['SALES_INFO_START_MONTH'] = df['BOM_INV_DATE'] - \
                pd.DateOffset(months=month-1)
            df['SALES_INFO_PERCENT_SKU_GM'] = df['SALES_INFO_GM_AMOUNT'] / \
                df['SALES_INFO_SOLD_AMOUNT']
        
            df['SALES_INFO_COGS_PER_UNIT'] = df['SALES_INFO_COGS'] / \
                df['SALES_INFO_QUANTITY_SOLD']
            df['SALES_INFO_TURNS'] = (
                df['SALES_INFO_COGS']/month*12)/df['TOTAL_COST_EXTENDED']
        
            df['SALES_INFO_PERCENT_SP_PER_UNIT_GM'] = 1 - (df['SALES_INFO_COGS_PER_UNIT']/(
                df['SALES_INFO_SOLD_AMOUNT']/df['SALES_INFO_QUANTITY_SOLD']))
        
            df['ADJ_QTY_DEMAND'] = df['QUANTITY_ON_HAND'].copy()
            df.loc[df['ADJ_QTY_DEMAND'].isna(), 'ADJ_QTY_DEMAND'] = df['TOTAL_COST_EXTENDED'] / \
                df['SALES_INFO_COGS_PER_UNIT']
        
            df['CPU'] = df['TOTAL_COST_EXTENDED']/df['ADJ_QTY_DEMAND']
        
            # df['percent_change_cpu'] = (
            #     df['CPU'] - df['SALES_INFO_COGS_PER_UNIT'])/df['CPU']SALES_INFO_PERCENT_COGS_CHANGE
            df['SALES_INFO_PERCENT_COGS_CHANGE'] = (
                 df['CPU'] - df['SALES_INFO_COGS_PER_UNIT'])/df['CPU']
            
            df.loc[(df['SALES_INFO_COGS_PER_UNIT']==0)|(df['CPU']==0),'SALES_INFO_PERCENT_COGS_CHANGE']=0 ##new condition 10/08
        
            df['SALES_INFO_PERCENT_COGS_CHANGE'] = df['SALES_INFO_PERCENT_COGS_CHANGE']*100
        
            df['ADJUSTED_QTY'] = df['SALES_INFO_QUANTITY_SOLD'].copy()
            df.index = range(len(df))
            df.loc[((df['SALES_INFO_PERCENT_COGS_CHANGE'] > 30) | (df['SALES_INFO_PERCENT_COGS_CHANGE'] < -30) |
                    (df['SALES_INFO_QUANTITY_SOLD'] == 0)), 'ADJUSTED_QTY'] = df['SALES_INFO_COGS']/df['CPU']
            if flag:
                df['SALES_INFO_SALES_QUANTITY_PER_MONTH'] = (
                    df['ADJUSTED_QTY']/month).fillna(0)
                # print('demand')
            else:
                df['SALES_INFO_SALES_QUANTITY_PER_MONTH'] = (
                    df['ADJUSTED_QTY']/df['SALES_INFO_MONTHS']).fillna(0)
                # print('demand_ann')
            
            # df.loc[df['BOM_INV_DATE'].isin(bom_list),zero_cols]=0   ###change imp
        
        
            df.loc[:, 'DEMAND_UNITS_NO_SALES'] = 0
            df.loc[df['SALES_INFO_SALES_QUANTITY_PER_MONTH'] ==
                    0, 'DEMAND_UNITS_NO_SALES'] = df['ADJ_QTY_DEMAND']
        
            return df
        
        
        def demand_unit_cost_cal(df, month):
            # df=demand.copy()
            # df.loc[:, 'DEMAND_UNITS_NO_SALES'] = 0
            # df.loc[df['SALES_INFO_SALES_QUANTITY_PER_MONTH'] ==
            #         0, 'DEMAND_UNITS_NO_SALES'] = df['ADJ_QTY_DEMAND']
            
            dict_onetime = ['DEMAND_UNITS_0_30', 'DEMAND_UNITS_31_60', 'DEMAND_UNITS_61_90', 'DEMAND_UNITS_91_180',
                            'DEMAND_UNITS_181_270', 'DEMAND_UNITS_271_360', 'DEMAND_UNITS_361_720', 'DEMAND_UNITS_720']
            demand.columns
            demand_unit = df[intesection(match_col, df_sales_map.columns)+[
                'ADJ_QTY_DEMAND', 'SALES_INFO_SALES_QUANTITY_PER_MONTH', 'DEMAND_UNITS_NO_SALES']]
            demand_unit1 = pd.DataFrame(columns=dict_onetime, dtype=object)
            # demand_unit2=pd.concat([demand_unit.iloc[:,:-1],pd.concat([demand_unit1], ignore_index=True),demand_unit.iloc[:,-1]],axis=1) ##test
            # demand_unit = pd.concat([demand_unit, pd.concat(
            #     [demand_unit1], ignore_index=True)], axis=1) error 
            demand_unit=pd.concat([demand_unit.iloc[:,:-1],pd.concat([demand_unit1], ignore_index=True),demand_unit.iloc[:,-1]],axis=1) ###solution  4/8 imp
        
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1 <= demand_unit['ADJ_QTY_DEMAND']) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_0_30'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1 > demand_unit['ADJ_QTY_DEMAND']) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_0_30'] = demand_unit['ADJ_QTY_DEMAND']*1
            demand_unit.loc[demand_unit['DEMAND_UNITS_0_30'].isna(),
                            'DEMAND_UNITS_0_30'] = 0
        
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1 <= (demand_unit['ADJ_QTY_DEMAND']-demand_unit['DEMAND_UNITS_0_30']))
                            & (demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_31_60'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1 > (demand_unit['ADJ_QTY_DEMAND']-demand_unit['DEMAND_UNITS_0_30'])) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_31_60'] = demand_unit['ADJ_QTY_DEMAND']-demand_unit['DEMAND_UNITS_0_30']
            demand_unit.loc[demand_unit['DEMAND_UNITS_31_60'].isna(),
                            'DEMAND_UNITS_31_60'] = 0
        
            sum1 = (demand_unit['DEMAND_UNITS_0_30']+demand_unit['DEMAND_UNITS_31_60'])
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1 <= (demand_unit['ADJ_QTY_DEMAND']-sum1)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_61_90'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1 > (demand_unit['ADJ_QTY_DEMAND']-sum1)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_61_90'] = (demand_unit['ADJ_QTY_DEMAND']-sum1)
            demand_unit.loc[demand_unit['DEMAND_UNITS_61_90'].isna(),
                            'DEMAND_UNITS_61_90'] = 0
        
            sum2 = (demand_unit['DEMAND_UNITS_0_30'] +
                    demand_unit['DEMAND_UNITS_31_60']+demand_unit['DEMAND_UNITS_61_90'])
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3 <= (demand_unit['ADJ_QTY_DEMAND']-sum2)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_91_180'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3 > (demand_unit['ADJ_QTY_DEMAND']-sum2)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_91_180'] = (demand_unit['ADJ_QTY_DEMAND']-sum2)
            demand_unit.loc[demand_unit['DEMAND_UNITS_91_180'].isna(),
                            'DEMAND_UNITS_91_180'] = 0
        
            sum3 = (demand_unit['DEMAND_UNITS_0_30']+demand_unit['DEMAND_UNITS_31_60'] +
                    demand_unit['DEMAND_UNITS_61_90']+demand_unit['DEMAND_UNITS_91_180'])
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3 <= (demand_unit['ADJ_QTY_DEMAND']-sum3)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_181_270'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3 > (demand_unit['ADJ_QTY_DEMAND']-sum3)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_181_270'] = (demand_unit['ADJ_QTY_DEMAND']-sum3)
            demand_unit.loc[demand_unit['DEMAND_UNITS_181_270'].isna(),
                            'DEMAND_UNITS_181_270'] = 0
        
            sum4 = (demand_unit['DEMAND_UNITS_0_30']+demand_unit['DEMAND_UNITS_31_60'] +
                    demand_unit['DEMAND_UNITS_61_90']+demand_unit['DEMAND_UNITS_91_180']+demand_unit['DEMAND_UNITS_181_270'])
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3 <= (demand_unit['ADJ_QTY_DEMAND']-sum4)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_271_360'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*3 > (demand_unit['ADJ_QTY_DEMAND']-sum4)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_271_360'] = (demand_unit['ADJ_QTY_DEMAND']-sum4)
            demand_unit.loc[demand_unit['DEMAND_UNITS_271_360'].isna(),
                            'DEMAND_UNITS_271_360'] = 0
        
            sum5 = (demand_unit['DEMAND_UNITS_0_30']+demand_unit['DEMAND_UNITS_31_60']+demand_unit['DEMAND_UNITS_61_90'] +
                    demand_unit['DEMAND_UNITS_91_180']+demand_unit['DEMAND_UNITS_181_270']+demand_unit['DEMAND_UNITS_271_360'])
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*12 <= (demand_unit['ADJ_QTY_DEMAND']-sum5)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_361_720'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*12
            demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*12 > (demand_unit['ADJ_QTY_DEMAND']-sum5)) & (
                demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH'] > 0), 'DEMAND_UNITS_361_720'] = (demand_unit['ADJ_QTY_DEMAND']-sum5)
            demand_unit.loc[demand_unit['DEMAND_UNITS_361_720'].isna(),
                            'DEMAND_UNITS_361_720'] = 0
        
            sum6 = (demand_unit['DEMAND_UNITS_0_30']+demand_unit['DEMAND_UNITS_31_60']+demand_unit['DEMAND_UNITS_61_90']+demand_unit['DEMAND_UNITS_91_180'] +
                    demand_unit['DEMAND_UNITS_181_270']+demand_unit['DEMAND_UNITS_271_360']+demand_unit['DEMAND_UNITS_361_720'] + demand_unit['DEMAND_UNITS_NO_SALES'])
            # sum6.fillna(0,inplace=True)
            #demand_unit.loc[(demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1<=(demand_unit['ADJ_QTY_DEMAND']-(sum6))) & (demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']>0), 'DEMAND_UNITS_720'] = demand_unit['SALES_INFO_SALES_QUANTITY_PER_MONTH']*1
            demand_unit.loc[:, 'DEMAND_UNITS_720'] = (
                demand_unit['ADJ_QTY_DEMAND']-sum6)
        
            demand_unit['DEMAND_UNITS_TOTAL'] = np.sum(
                demand_unit.loc[:, [i for i in demand_unit.columns if 'UNITS' in i]], axis=1)
        
            demand_unit['DEMAND_UNITS_CHECK'] = demand_unit['DEMAND_UNITS_TOTAL'] - \
                demand_unit['ADJ_QTY_DEMAND']
        
            cost_per_unit = demand['CPU'].copy()
            demand_cost = cost_per_unit.values[:, None]*demand_unit.loc[:,
                                                                        [i for i in demand_unit.columns if 'UNITS' in i]].values
        
            demand_cost_df = pd.DataFrame(demand_cost, columns=[i.replace('_UNITS_', '_COST_AMOUNT_') for i in dict_onetime]+[
                                          'DEMAND_COST_NO_SALES_AMOUNT', 'DEMAND_COST_TOTAL_AMOUNT', 'DEMAND_COST_CHECK_AMOUNT'])
        
            demand_cost_df['CPU'] = cost_per_unit
            for i in intesection(match_col, df_sales_map.columns):
                demand_cost_df[i] = demand[i].copy()
        
            sos_unit = pd.DataFrame()
            for i in intesection(match_col, df_sales_map.columns):
                sos_unit[i] = demand[i].copy()
        
            #sos_unit['Frame + SKU Consolidation'] = demand['Frame + SKU Consolidation'].copy()
            sos_unit['STOCK_OUT_SALES_UNIT_SALES_PER_WEEK'] = (
                demand['SALES_INFO_SALES_QUANTITY_PER_MONTH'].copy()/30)*7
            sos_unit['ADJ_QTY_DEMAND'] = demand['ADJ_QTY_DEMAND'].copy()
        
            sos_unit['STOCK_OUT_SALES_UNIT_'+str(1)+'_WEEK'] = np.minimum(
                sos_unit['STOCK_OUT_SALES_UNIT_SALES_PER_WEEK'], sos_unit['ADJ_QTY_DEMAND'])
            total = 0
        
            for i in range(2, 17):
                total = total + sos_unit['STOCK_OUT_SALES_UNIT_'+str(i-1)+'_WEEK']
               
                sos_unit.loc[((sos_unit['ADJ_QTY_DEMAND'] - total) > 0) & (sos_unit['STOCK_OUT_SALES_UNIT_SALES_PER_WEEK'] > 0), 'STOCK_OUT_SALES_UNIT_' +
                             str(i)+'_WEEK'] = np.minimum(sos_unit['STOCK_OUT_SALES_UNIT_SALES_PER_WEEK'], sos_unit['ADJ_QTY_DEMAND'] - total) #####pj
        
        
            cost_per_unit = demand['CPU'].copy()
            sos_cost = cost_per_unit.values[:, None]*sos_unit.loc[:, [
                i for i in sos_unit.columns if 'STOCK_OUT_SALES_UNIT' in i]].values
        
            import re
        
            def has_numbers(inputString):
                return bool(re.search(r'\d', inputString))
            sos_cost_df = pd.DataFrame(sos_cost, columns=['STOCK_OUT_SALES_COST_COGS_PER_WEEK'] + [
                                       i.replace('_UNIT_', '_COST_')+'_AMOUNT' for i in sos_unit.columns if has_numbers(i)])
        
            sos_cost_df['CPU'] = cost_per_unit
            for i in intesection(match_col, df_sales_map.columns):
                sos_cost_df[i] = demand[i].copy()
        
            df = pd.concat([df, demand_unit, demand_cost_df,
                           sos_unit, sos_cost_df], axis=1)
        
            df = df.loc[:, ~df.columns.duplicated()]
        
            return df
            
            
        
        months = [2,3,6,12,24]
        dict_month = {2:'TWO', 3:'THREE', 6:'SIX', 12:'TWELVE', 24:'TWENTYFOUR'}
        db_table = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                     sheet_name=f'FACT_TWO_MONTH_INVENTORY')
        db_table1 = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                     sheet_name=f'FACT_TWO_MONTH_QUINTILE')
        
        for month in months:
        
            # month = 2  ##comment while testing
            match_col = ['LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE', 'TRIM_SKU']
        
            agg_inv_col = ['QUANTITY_ON_HAND', 'TOTAL_COST_EXTENDED']
            agg_sale_col = ['QUANTITY_SOLD', 'NET_SALES_$', 'GROSS_MARGIN_$', 'COGS_$'] #sometimes agg columns are missed in the raw file
            
            df_inv_map['BOM_INV_DATE'] = pd.to_datetime(df_inv_map['BOM_INV_DATE'])
            df_sales_map['BEG_MO'] = pd.to_datetime(df_sales_map['BEG_MO'])
            
            measure_invt = df_inv_map.groupby(intesection(match_col, df_inv_map.columns) + [
                                              'BOM_INV_DATE'])[intesection(agg_inv_col, df_inv_map.columns)].sum().reset_index()
            measure_sale = df_sales_map.groupby(intesection(match_col, df_sales_map.columns) + [
                                                'BEG_MO'])[intesection(agg_sale_col, df_sales_map.columns)].sum().reset_index()
            
            first_receipt = measure_invt.groupby(intesection(match_col, df_inv_map.columns))[
                'BOM_INV_DATE'].min().reset_index()
            first_receipt = first_receipt.rename(
                {'BOM_INV_DATE': 'FIRST_RECEIPT_DATE'}, axis=1)
        
        #last_receipt = measure_invt.groupby(intesection(match_col, df_inv_map.columns))['BOM_INV_DATE'].max().reset_index()
        #last_receipt = last_receipt.rename({'BOM_INV_DATE':'LAST_RECEIPT_DATE'},axis=1)
        
            measure_invt = pd.merge(measure_invt, first_receipt,
                                    how='left', on=intesection(match_col, df_inv_map.columns))# to get first receipt date 
            
            # measure_invt = pd.merge(measure_invt, measure_sale[["TRIM_SKU", "BEG_MO"]], how = "left", on = intesection(match_col, df_inv_map.columns)) #change made by manoj
            measure_invt = pd.merge(measure_invt, measure_sale[intesection(match_col, df_sales_map.columns)+['BEG_MO']], how = "left", on = intesection(match_col, df_inv_map.columns)) #change made by manoj ##9/8
            
            measure_invt = measure_invt.drop_duplicates(subset = intesection(match_col, df_inv_map.columns) + [
                                              'BOM_INV_DATE'], ignore_index = True)  #change made by manoj
            BGMO_UNIQUE = pd.DataFrame() #change made by manoj
            BGMO_UNIQUE["BOM_INV_DATE"] = measure_invt["BEG_MO"].unique() #change made by manoj
            BGMO_UNIQUE["mon_start_avail_Seq_id1"] = 1 #change made by manoj
            BGMO_UNIQUE = BGMO_UNIQUE[pd.notnull(BGMO_UNIQUE["BOM_INV_DATE"])] #change made by manoj
            
            
            
            measure_invt = pd.merge(measure_invt, BGMO_UNIQUE, how = "left", on = "BOM_INV_DATE") #change made by manoj
            
            BGMO_UNIQUE = BGMO_UNIQUE.rename({"BOM_INV_DATE":"BOM_INV_DATE1", "mon_start_avail_Seq_id1" : "mon_start_avail_Seq_id2"}, axis = 1) #change made by manoj
            measure_invt["BOM_INV_DATE1"] = measure_invt["BOM_INV_DATE"].apply(lambda x: x - dateutil.relativedelta.relativedelta(months=month-1)) #change made by manoj
            
            
            measure_invt = pd.merge(measure_invt, BGMO_UNIQUE, how = "left", on = "BOM_INV_DATE1") #change made by manoj
            
            measure_invt["mon_start_avail_Seq_id"] = "dnu" #change made by manoj
            measure_invt.loc[measure_invt["mon_start_avail_Seq_id1"]+measure_invt["mon_start_avail_Seq_id2"]==2, "mon_start_avail_Seq_id"] = "use"  #change made by manoj
            measure_invt.drop(["mon_start_avail_Seq_id1", "mon_start_avail_Seq_id2",  "BOM_INV_DATE1"], axis = 1, inplace = True)    #change made by manoj
            measure_invt.loc[measure_invt.BEG_MO.isnull(), "mon_start_avail_Seq_id"] = "use"   #change made by manoj
            
            
            first_sale = measure_sale.groupby(intesection(match_col, df_sales_map.columns))[
                'BEG_MO'].min().reset_index()
            first_sale = first_sale.rename({'BEG_MO': 'FIRST_SALE_DATE'}, axis=1)
        
        #first_sale = measure_sale.groupby(intesection(match_col, df_sales_map.columns))['BEG_MO'].min().reset_index()
        #first_sale = first_sale.rename({'BEG_MO':'FIRST_SALE_DATE'},axis=1)
        
            measure_sale = pd.merge(measure_sale, first_sale, how='left',
                                    on=intesection(match_col, df_sales_map.columns))
            
            agg_dict = {}
            for i in intesection(agg_sale_col, df_sales_map.columns):
                agg_dict[i] = 'sum'
            
            agg_dict['FIRST_SALE_DATE'] = 'min'
            
            final_measure_invt = pd.DataFrame()
            for res in sorted(list(set(measure_invt['BOM_INV_DATE']))):# in demand calculation is done on all the months
            
                # measure_sale_latest = measure_sale[(measure_sale['BEG_MO'] == res) | (
                #     measure_sale['BEG_MO'] == res-dateutil.relativedelta.relativedelta(months=month-1))]
                start_date=res-dateutil.relativedelta.relativedelta(months=month-1)
                # measure_sale_latest = measure_sale[measure_sale['BEG_MO'].isin([start_date,res])] ###change 5/8 ##incorrect
                measure_sale_latest = measure_sale.loc[(measure_sale['BEG_MO']>=start_date)&(measure_sale['BEG_MO']<=res)]
                #measure_sale_latest = measure_sale[(measure_sale['BEG_MO'] >= res-dateutil.relativedelta.relativedelta(months=month-1))]
                
                measure_sale_latest = measure_sale_latest.groupby(intesection(
                    match_col, df_sales_map.columns)).agg(agg_dict).reset_index()
                # sorted(measure_sale['BEG_MO'].unique())
               # measure_sale_latest = pd.merge(measure_sale_latest, measure_sale["BEG_MO", "TRIM_SKU"], on = "TRIM_SKU", how = "left")
                measure_sale_latest['BOM_INV_DATE'] = res
                temp_measure_invt = pd.merge(measure_sale_latest, measure_invt, on=intesection(
                    match_col, df_sales_map.columns)+['BOM_INV_DATE'], how='inner')
            
                final_measure_invt = final_measure_invt.append(temp_measure_invt)
            
            final_measure_invt = pd.merge(final_measure_invt, measure_invt, on=[
                                          'BOM_INV_DATE']+intesection(match_col, df_sales_map.columns), how='right')
            ###5/8#############
           
            ###################
            l1 = []
            l2 = []
            for i in final_measure_invt.columns:
                if ('_x' in i):
                    l1.append(i)
                elif ('_y' in i):
                    l2.append(i)
            final_measure_invt.drop(l1, axis=1, inplace=True)
            final_measure_invt.columns = [
                i.replace('_y', '') for i in final_measure_invt.columns]
            
            demand = final_measure_invt.copy()
            
            # SALES_INFO_MONTHS
            demand = demand.rename({
                'QUANTITY_SOLD': 'SALES_INFO_QUANTITY_SOLD',
                'NET_SALES_$': 'SALES_INFO_SOLD_AMOUNT',
                'COGS_$': 'SALES_INFO_COGS',
                'GROSS_MARGIN_$': 'SALES_INFO_GM_AMOUNT'}, axis=1)
            
            demand['INTRODUCTION_DATE'] = demand[[
                'FIRST_SALE_DATE', 'FIRST_RECEIPT_DATE']].min(axis=1)
        
            demand.loc[((demand['BOM_INV_DATE']-demand['INTRODUCTION_DATE']).dt.days+30 <= month*(365/12)),
                       '2M Days Sales Annualized'] = (demand['BOM_INV_DATE']-demand['INTRODUCTION_DATE']).dt.days+30
            demand.loc[((demand['BOM_INV_DATE']-demand['INTRODUCTION_DATE']).dt.days +
                        30 > month*(365/12)), '2M Days Sales Annualized'] = month*(365/12)
            
            demand['SALES_INFO_MONTHS'] = demand['2M Days Sales Annualized']/(365/12)# this dataframe is used to calculate demand feilds
            demand['SALES_INFO_MONTHS']=np.round(demand['SALES_INFO_MONTHS'])
            demand['SALES_INFO_MONTHS'].unique()
            demand1=demand.copy()
            ##### testing 04-08-22
            # zero_cols=['SALES_INFO_QUANTITY_SOLD','SALES_INFO_SOLD_AMOUNT','SALES_INFO_COGS',
            #  'SALES_INFO_GM_AMOUNT']
            zero_cols=['SALES_INFO_QUANTITY_SOLD','SALES_INFO_SOLD_AMOUNT','SALES_INFO_COGS',
                       'SALES_INFO_GM_AMOUNT','SALES_INFO_PERCENT_SKU_GM','SALES_INFO_PERCENT_SP_PER_UNIT_GM',
                       'SALES_INFO_TURNS','SALES_INFO_SALES_QUANTITY_PER_MONTH','SALES_INFO_COGS_PER_UNIT',
                       'SALES_INFO_PERCENT_COGS_CHANGE']  # SALES_INFO_CATEGORYWISE_PERCENT_GM is not getting calculated
            # demand.dtypes
            # start_date=demand['BOM_INV_DATE'].min()-dateutil.relativedelta.relativedelta(months=month-1)
            # res-relativedelta(months=month-1)
            # a=demand[demand['BEG_MO']==demand['BOM_INV_DATE'].min()]
            # res=sorted(list(set(demand['BOM_INV_DATE'])))[3]
            # demand[demand['BEG_MO']==res]
            # b=demand[demand['BOM_INV_DATE']==res]
        
            # sorted(demand['BEG_MO'].unique())
            # month=2
            # df_sales_map[df_sales_map['BEG_MO']==res]
            
            # demand[demand['BEG_MO']==res]
            # bom_list=sorted(list(set(demand['BOM_INV_DATE'])))
            # bom_list[:(month-1)]
            # demand.loc[demand['BOM_INV_DATE'].isin(bom_list[:(month-1)]),['SALES_INFO_QUANTITY_SOLD',
            #     'SALES_INFO_SOLD_AMOUNT','SALES_INFO_COGS', 'SALES_INFO_GM_AMOUNT']]=0
            
            # for res in sorted(list(set(demand['BOM_INV_DATE']))):
                # print(res)
                # type(res)
                # res.date()
                # type(dateutil.relativedelta.relativedelta(months=month-1))
                # start_date=res-dateutil.relativedelta.relativedelta(months=month-1)
                # if len(demand[demand['BOM_INV_DATE']==start_date])==0:
                #     demand.loc[demand['BOM_INV_DATE']==res,'make_zero']='dnu'
                # else:
                #     demand.loc[demand['BOM_INV_DATE']==res,'make_zero']='use'
            
            # demand[demand['make_zero']=='no_use']['BOM_INV_DATE'].unique()
            #####
            bom_list=sorted(list(set(demand['BOM_INV_DATE']))) ###change
            bom_list[:(month-1)]##change
            demand = demand_calculation(demand, True, month,bom_list[:(month-1)],zero_cols)
            demand_ann = demand_calculation(demand1, False, month,bom_list[:(month-1)],zero_cols)
            
            # demand.loc[demand['BOM_INV_DATE'].isin(bom_list[:(month-1)])]  ####test
            # demand.loc[demand['BOM_INV_DATE'].isin(bom_list[:(month-1)]),zero_cols]=0   ###imp###
            
            # demand.loc['DEMAND_UNITS_NO_SALES']=0
            # demand.loc[demand['SALES_INFO_SALES_QUANTITY_PER_MONTH'] ==
            #         0, 'DEMAND_UNITS_NO_SALES'] = demand['ADJ_QTY_DEMAND']
            
            # demand.loc[demand['BOM_INV_DATE'].isin(bom_list[:(month-1)])]['SALES_INFO_SALES_QUANTITY_PER_MONTH'].sum() ###test
            
            
            demand = demand_unit_cost_cal(demand, month)
           
            demand_ann = demand_unit_cost_cal(demand_ann, month)
            demand['TRIM_SKU']=demand['TRIM_SKU'].astype(str)
            demand['SKU']=demand['TRIM_SKU'] ###test  same in ann
            demand["DATE_KEY"]=demand['BOM_INV_DATE'].dt.date.astype(str).str.replace('-','')
            
            demand_ann['TRIM_SKU']=demand_ann['TRIM_SKU'].astype(str)
        
            demand_ann['SKU']=demand_ann['TRIM_SKU'] ###test  same in ann
            demand_ann["DATE_KEY"]=demand_ann['BOM_INV_DATE'].dt.date.astype(str).str.replace('-','')
            # mar=demand.loc[demand['DATE_KEY']=='20220301']
            # feb=demand.loc[demand['DATE_KEY']=='20220201']
            # may=demand.loc[demand['DATE_KEY']=='20220501']
            # apr=demand.loc[demand['DATE_KEY']=='20200401']
            # demand[demand['DATE_KEY']=='20200401']['SALES_INFO_SALES_QUANTITY_PER_MONTH'].sum()
            # jun=demand.loc[demand['DATE_KEY']=='20220601']
            # etl_2m=pd.read_csv('2m_10799_ETL (1).csv')
            # junetl=etl_2m[etl_2m['DATE_KEY']==20220601]
            # junetl.sort_values('DEMAND_UNITS_0_30',ascending=False,inplace=True)
            # jun.sort_values('DEMAND_UNITS_0_30',ascending=False,inplace=True)
            # jun=jun.reset_index()
            # junetl=junetl.reset_index()
            # error=jun.loc[jun['DEMAND_UNITS_0_30']!=junetl['DEMAND_UNITS_0_30']]
            # error_etl=junetl.loc[jun['DEMAND_UNITS_0_30']!=junetl['DEMAND_UNITS_0_30']]
            # demand.to_excel('demand_6_mon_py_10799.xlsx')
            ###############
            #just checking units and cost amount for specific datekey in demand
            # def check_units_and_cost(df,date_key):
            #     print(date_key)
            #     df=df.loc[df['DATE_KEY']==date_key]
            #     print(df[[i for i in df.columns if 'DEMAND_UNITS' in i]].sum())
        
            #     print(df[[i for i in df.columns if 'DEMAND_COST' in i]].sum())
            # check_units_and_cost(demand,'20220501')
        
            ###############
            # demand["DATE_KEY"] = pd.to_datetime(demand['BOM_INV_DATE']).dt.date.apply(lambda x: int(str(x).replace('-',''))) ###test
            db_demand = pd.DataFrame(columns=db_table.iloc[:,1].tolist(), dtype=object)
            db_demand[intesection(demand.columns, db_demand.columns)] = demand[intesection(
                demand.columns, db_demand.columns)].copy()
            
            # db_demand[db_demand['DATE_KEY']=='20200401']
            # db_demand[db_demand['DATE_KEY']=='20200401']['SALES_INFO_SALES_QUANTITY_PER_MONTH'].sum()
            # db_demand['DATE_KEY'].unique()
            # check_units_and_cost(db_demand,'20220501')
        
            # db_demand['SALES_INFO_SALES_QUANTITY_PER_MONTH'].sum() ###test
            db_demand['TWO_MONTH_INVENTORY_SEQ_ID'] = np.random.randint(
                10000, 10000000000000, len(db_demand), dtype=np.int64)
            db_dim_sku1 = db_dim_sku.copy()
            db_dim_sku1 = db_dim_sku1.rename({'SKU': 'TRIM_SKU'}, axis=1)
            db_dim_sku1 = db_dim_sku1.loc[:, ~db_dim_sku1.columns.duplicated()]
        #    demand
            db_demand['SKU_SEQ_ID'] = pd.merge(demand, db_dim_sku1[intesection(match_col, df_sales_map.columns)+[
                                               'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map.columns), how='left')['SKU_SEQ_ID']
            # db_demand['SKU_SEQ_ID'] = pd.merge(demand, db_dim_sku1[intesection(match_col, df_sales_map.columns)+[
                                               # 'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map.columns), how='left')['SKU_SEQ_ID'] ##test
        #    db_demand['INTRODUCTION_DATE']
        #    inv_bkt.drop('SKU_SEQ_ID',axis=1,inplace=True)
        #    inv_bkt['SKU_SEQ_ID'] = pd.merge(inv_bkt, db_dim_sku1[intesection(match_col, df_sales_map.columns)+[
        #                                       'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map.columns), how='left')['SKU_SEQ_ID']
        ##    fact_inv_db['BOM_INV_DATE']
        #    db_demand = len(pd.merge(demand[['SKU_SEQ_ID','BOM_INV_DATE']], inv_bkt, on = ['SKU_SEQ_ID','BOM_INV_DATE'], how='left'))
        #    db_demand = temp_bucket(db_demand, 'inv', deal_id, flag_fact=True)
        #    db_demand = temp_bucket(db_demand, 'sale', deal_id, flag_fact=True)
        
            # print([i for i in fact_inv_db.columns if 'DATE' in i])
            # print([i for i in fact_inv_db.columns if ('DATE' in i) or ('BUCKET' in i)])
            # pd.merge(db_demand,fact_inv_db[[i for i in fact_inv_db.columns if ('DATE' in i) or ('BUCKET' in i)]+['SKU_SEQ_ID']],
            #           on=['SKU_SEQ_ID'],how='left')
            # db_demand.isna().sum()
            # date_bucket=[i for i in fact_inv_db.columns if ('DATE' in i) or ('BUCKET' in i)]
            # set(db_demand.columns).intersection(set(date_bucket))
            db_demand.sort_values(['SKU_SEQ_ID','DATE_KEY'],inplace=True)
            fact_inv_db_copy=fact_inv_db.copy()  ##3
            # db_demand.set_index('')
            fact_inv_db_copy.sort_values(['SKU_SEQ_ID','DATE_KEY'],inplace=True)
            db_demand.reset_index(drop=True,inplace=True)
            fact_inv_db_copy.reset_index(drop=True,inplace=True)
        
            # db_demand.isna().sum() ##
            db_demand[[i for i in fact_inv_db_copy.columns if 'DATE' in i]] = fact_inv_db_copy[[i for i in fact_inv_db_copy.columns if 'DATE' in i]]
            
            db_demand['INTRODUCTION_BUCKET'] = fact_inv_db_copy['INTRODUCTION_BUCKET'].tolist() 
            
            # db_demand['DATE_KEY'] = fact_inv_db_copy['DATE_KEY'].tolist() ## no need
        
            # db_demand['SKU'] = fact_inv_db_copy['SKU'].tolist() ## no need
            if 'CATEGORY_1' in fact_inv_db_copy.columns:    ####no category in fact_inv_db
                db_demand['CATEGORY_1'] = fact_inv_db_copy['CATEGORY_1'].tolist()
        
            db_demand['GROSS_INVENTORY_QUANTITY'] = fact_inv_db_copy['QUANTITY_ON_HAND'].tolist()
        
            db_demand['GROSS_INVENTORY_COST'] = fact_inv_db_copy['TOTAL_COST_EXTENDED'].tolist()
        
            if 'ORIGINAL_RETAIL_PRICE_EXTENDED' in fact_inv_db_copy.columns:
                db_demand['ORIGINAL_RETAIL_PRICE_EXTENDED'] = fact_inv_db_copy['ORIGINAL_RETAIL_PRICE_EXTENDED'].tolist()
            
            db_demand = db_demand.applymap(
                lambda x: x.date() if isinstance(x, datetime) else x)
            
            db_demand.replace([np.inf, -np.inf], np.nan, inplace=True)
            db_demand['INSERT_DATE'] = datetime.now().date()
            db_demand.drop('TWO_MONTH_INVENTORY_SEQ_ID', axis=1, inplace=True)
            
            db_demand['DEAL_ID'] = deal_id
        
            db_demand = db_demand[db_table.iloc[:,1].tolist()[1:]]
            db_demand.dropna(axis=1, how='all',inplace=True)
            db_demand.replace('nan',0, inplace=True)
            db_demand.replace(np.nan,0, inplace=True)
            # db_demand[db_demand['DATE_KEY']=='20200401']['SALES_INFO_SALES_QUANTITY_PER_MONTH'].sum()
            s1 = time.time()
            #success, nchunks, nrows, _ = write_pandas(ctx, renaming(db_demand), f'FACT_{dict_month[month]}_MONTH_INVENTORY')
            s2 = time.time()
            total_insert_time = total_insert_time + (s2-s1)
            
            db_demand_ann = pd.DataFrame(columns=db_table.iloc[:,1].tolist(), dtype=object)
            db_demand_ann[intesection(demand_ann.columns, db_demand_ann.columns)] = demand_ann[intesection(
                demand_ann.columns, db_demand_ann.columns)].copy()
            
        #    db_demand_ann['TWO_MONTH_INVENTORY_SEQ_ID'] = np.random.randint(
        #        10000, 10000000000000, len(db_demand_ann), dtype=np.int64)
            db_dim_sku1 = db_dim_sku.copy()
            db_dim_sku1 = db_dim_sku1.rename({'SKU': 'TRIM_SKU'}, axis=1)
            db_dim_sku1 = db_dim_sku1.loc[:, ~db_dim_sku1.columns.duplicated()]
        #    demand
            db_demand_ann['SKU_SEQ_ID'] = pd.merge(demand_ann, db_dim_sku1[intesection(match_col, df_sales_map.columns)+[
                                               'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map.columns), how='left')['SKU_SEQ_ID']
        #    db_demand['INTRODUCTION_DATE']
        #    inv_bkt.drop('SKU_SEQ_ID',axis=1,inplace=True)
        #    inv_bkt['SKU_SEQ_ID'] = pd.merge(inv_bkt, db_dim_sku1[intesection(match_col, df_sales_map.columns)+[
        #                                       'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map.columns), how='left')['SKU_SEQ_ID']
        ##    fact_inv_db['BOM_INV_DATE']
        #    db_demand = len(pd.merge(demand[['SKU_SEQ_ID','BOM_INV_DATE']], inv_bkt, on = ['SKU_SEQ_ID','BOM_INV_DATE'], how='left'))
        #    db_demand = temp_bucket(db_demand, 'inv', deal_id, flag_fact=True)
        #    db_demand = temp_bucket(db_demand, 'sale', deal_id, flag_fact=True)
            db_demand_ann.sort_values(['SKU_SEQ_ID','DATE_KEY'],inplace=True)
            # fact_inv_db.sort_values('SKU_SEQ_ID',inplace=True)
            fact_inv_db_copy=fact_inv_db.copy()  ##3
            fact_inv_db_copy.sort_values(['SKU_SEQ_ID','DATE_KEY'],inplace=True)
            
            db_demand.reset_index(drop=True,inplace=True)
            fact_inv_db_copy.reset_index(drop=True,inplace=True)
            
            db_demand_ann[[i for i in fact_inv_db_copy.columns if 'DATE' in i]] = fact_inv_db_copy[[i for i in fact_inv_db_copy.columns if 'DATE' in i]]
            
            db_demand_ann['INTRODUCTION_BUCKET'] = fact_inv_db_copy['INTRODUCTION_BUCKET'].tolist()
            
            # db_demand_ann['DATE_KEY'] = fact_inv_db_copy['DATE_KEY'].tolist()
        
            # db_demand_ann['SKU'] = fact_inv_db_copy['SKU'].tolist() ##4/8
            if 'CATEGORY_1' in fact_inv_db_copy.columns:
                db_demand_ann['CATEGORY_1'] = fact_inv_db_copy['CATEGORY_1'].tolist()
        
            db_demand_ann['GROSS_INVENTORY_QUANTITY'] = fact_inv_db_copy['QUANTITY_ON_HAND'].tolist()
        
            db_demand_ann['GROSS_INVENTORY_COST'] = fact_inv_db_copy['TOTAL_COST_EXTENDED'].tolist()
        
            if 'ORIGINAL_RETAIL_PRICE_EXTENDED' in fact_inv_db_copy.columns:
                db_demand_ann['ORIGINAL_RETAIL_PRICE_EXTENDED'] = fact_inv_db_copy['ORIGINAL_RETAIL_PRICE_EXTENDED'].tolist()
            
            db_demand_ann = db_demand_ann.applymap(
                lambda x: x.date() if isinstance(x, datetime) else x)
            
            db_demand_ann.replace([np.inf, -np.inf], np.nan, inplace=True)
            db_demand_ann['INSERT_DATE'] = datetime.now().date()
            db_demand_ann.drop('TWO_MONTH_INVENTORY_SEQ_ID', axis=1, inplace=True)
            
            db_demand_ann['DEAL_ID'] = deal_id
        
            db_demand_ann = db_demand_ann[db_table.iloc[:,1].tolist()[1:]]
            db_demand_ann.dropna(axis=1, how='all',inplace=True)
            db_demand_ann.replace('nan',0, inplace=True)
            db_demand_ann.replace(np.nan,0, inplace=True)
            
            # s1 = time.time()
            db_demand_ann_copy = db_demand_ann.copy()  #change made by manoj
            db_demand_ann_copy = db_demand_ann_copy.rename({"FIRST_RECEIPT_DATE_BUCKET":"FIRST_RECEIPT_BUCKET_SEQ_ID"}, axis = 1)  #change made by manoj
            s1 = time.time()
            #success, nchunks, nrows, _ = write_pandas(ctx, renaming(db_demand_ann_copy), f'FACT_{dict_month[month]}_MONTH_ANNUALIZED')
            s2 = time.time()
            total_insert_time = total_insert_time + (s2-s1)
            # set(db_demand.columns).difference(set(db_demand_ann.columns))
            # db_demand.to_excel('2m_demand_10795_updt.xlsx')
            
            # a=db_demand.loc[(db_demand['DATE_KEY']=='20220301')&(db_demand['SKU']=='2-ZIRAH492')]
            # db_demand['DATE_KEY'].max()
            # df.loc[(df['TRIM_SKU']=='2-ZIRAH492')]
            # demand[demand['TRIM_SKU']=='G CLIP WHITE']
            # df_inv_map[df_inv_map['TRIM_SKU']=='G CLIP WHITE']
        print('''###############################################
        
        '6. QUINTILE'
        
        ###############################################''')
        
        months = [2,3,6,12,24]
        dict_month = {2:'TWO', 3:'THREE', 6:'SIX', 12:'TWELVE', 24:'TWENTYFOUR'}
        match_col = ['LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE', 'TRIM_SKU']
        db_table1 = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                     sheet_name=f'FACT_TWO_MONTH_QUINTILE')
        for month in months:
            # month=2  ##comment while testing
            quintile_latest = df_sales_map.rename({
                'QUANTITY_SOLD': 'SALES_INFO_QUANTITY_SOLD',
                'NET_SALES_$': 'SALES_INFO_SOLD_AMOUNT',
                'COGS_$': 'SALES_INFO_COGS',
                'GROSS_MARGIN_$': 'SALES_INFO_GM_AMOUNT'}, axis=1)
            
        
            res=max(pd.to_datetime(quintile_latest['BEG_MO'])).date()
            quintile_latest = quintile_latest.sort_values(
                'SALES_INFO_SOLD_AMOUNT', ascending=False)
        
            quintile_latest_grp_sale = quintile_latest.groupby(intesection(match_col, df_sales_map.columns)+['BEG_MO'])['SALES_INFO_QUANTITY_SOLD','SALES_INFO_SOLD_AMOUNT', 'SALES_INFO_COGS', 'SALES_INFO_GM_AMOUNT'].sum().reset_index()
            quintile_latest_grp_inv = df_inv_map.groupby(intesection(match_col, df_inv_map.columns)+['BOM_INV_DATE'])['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED'].sum().reset_index()
            quintile_latest_grp_inv = quintile_latest_grp_inv.rename({'BOM_INV_DATE':'BOM'}, axis=1)
            quintile_latest_grp_sale = quintile_latest_grp_sale.rename({'BEG_MO':'BOM'}, axis=1)
            quintile_latest_grp_inv['BOM'] = pd.to_datetime(quintile_latest_grp_inv['BOM']).dt.date
            quintile_latest_grp_sale['BOM'] = pd.to_datetime(quintile_latest_grp_sale['BOM']).dt.date
            quintile_latest_grp = pd.merge(quintile_latest_grp_sale, quintile_latest_grp_inv, on=intesection(match_col, df_inv_map.columns)+['BOM'], how='left')
            
            quintile_latest_grp.isna().sum()
        
            quintile_latest_grp_1=quintile_latest_grp.loc[(quintile_latest_grp['BOM']>=res-dateutil.relativedelta.relativedelta(months=month-1))]
        
            quintile_latest_grp_2 = quintile_latest_grp_inv[(quintile_latest_grp_inv['BOM'] == res)] # for latest month
           
            quintile_latest_grp_1 = quintile_latest_grp_1.groupby(intesection(match_col, df_inv_map.columns))['SALES_INFO_QUANTITY_SOLD',
                        'SALES_INFO_SOLD_AMOUNT', 'SALES_INFO_COGS', 'SALES_INFO_GM_AMOUNT'].sum()
          
            quintile_latest_grp_1 = pd.merge(quintile_latest_grp_2[intesection(match_col, df_inv_map.columns)+['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED']], quintile_latest_grp_1, on=intesection(match_col, df_inv_map.columns), how='right')
            
            quintile_latest_grp_1 = quintile_latest_grp_1.sort_values('SALES_INFO_SOLD_AMOUNT',ascending=False)
            quintile_latest_grp_2['TOTAL_COST_EXTENDED'].sum()  ##comment while testing
            
            sum_of_net_sales = sum(quintile_latest_grp_1.loc[~((quintile_latest_grp_1['TOTAL_COST_EXTENDED'].isna()) | (
                quintile_latest_grp_1['TOTAL_COST_EXTENDED'] == 0)), 'SALES_INFO_SOLD_AMOUNT'])
            
            quintile_latest_grp_1['PERCENT'] = quintile_latest_grp_1['SALES_INFO_SOLD_AMOUNT']/sum_of_net_sales
            
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['TOTAL_COST_EXTENDED'].isna()) | (
                quintile_latest_grp_1['TOTAL_COST_EXTENDED'] == 0), 'PERCENT'] = 0
            
            quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] = np.cumsum(
                quintile_latest_grp_1['PERCENT'])
            
            quintile_latest_grp_1['CPU'] = quintile_latest_grp_1['TOTAL_COST_EXTENDED'] / \
                quintile_latest_grp_1['QUANTITY_ON_HAND']
            quintile_latest_grp_1.replace([np.inf, -np.inf], 0, inplace=True)
            
            quintile_latest_grp_1['SKU'] = 1
            quintile_latest_grp_1.loc[quintile_latest_grp_1['TOTAL_COST_EXTENDED'] == 0, 'SKU'] = 0
            
            quintile_latest_grp_1['INVENTORY_PERCENT'] = quintile_latest_grp_1['TOTAL_COST_EXTENDED'] / \
                np.nansum(quintile_latest_grp_1['TOTAL_COST_EXTENDED'])
                
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['INVENTORY_PERCENT'].isna()) | (
                np.isinf(quintile_latest_grp_1['INVENTORY_PERCENT'])), 'INVENTORY_PERCENT'] = 0
            
            
            quintile_latest_grp_1['INVENTORY_CUM_PERCENT'] = np.cumsum(
                quintile_latest_grp_1['INVENTORY_PERCENT'])
            
            quintile_latest_grp_1.loc[quintile_latest_grp_1['COMMULATIVE_PERCENTAGE']
                                      <= 0.2, 'QUINTILE'] = 1
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] > 0.2) & (
                quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] <= 0.4), 'QUINTILE'] = 2
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] > 0.4) & (
                quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] <= 0.6), 'QUINTILE'] = 3
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] > 0.6) & (
                quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] <= 0.8), 'QUINTILE'] = 4
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] > 0.8) & (
                quintile_latest_grp_1['COMMULATIVE_PERCENTAGE'] <= 1), 'QUINTILE'] = 5
            quintile_latest_grp_1.loc[(quintile_latest_grp_1['TOTAL_COST_EXTENDED'].isna())|(quintile_latest_grp_1['TOTAL_COST_EXTENDED']==0), 'QUINTILE'] = 'Sales No Inv'  ##changes 10/08
        #    quintile_latest_grp_1.drop(intesection(match_col, df_inv_map.columns), axis=1,inplace=True)
            
            quintile_latest_grp_1 = quintile_latest_grp_1.rename({
                 'SALES_INFO_QUANTITY_SOLD':'QUANTITY_SOLD',
                 'SALES_INFO_SOLD_AMOUNT':'NET_SALES_$',
                 'SALES_INFO_COGS':'COGS',
                 'SALES_INFO_GM_AMOUNT':'GROSS_MARGIN_$',
                 'QUANTITY_ON_HAND':'QUANTITY',
                 'TOTAL_COST_EXTENDED':'GROSS_INVENTORY_COST'}, axis=1)
            
            quintile_latest_grp_1['INSERT_DATE'] = datetime.now().date()
                
            quintile_latest_grp_1['DATE_KEY'] = pd.to_datetime(quintile_latest['BEG_MO']).dt.date.astype(str).str.replace('-','')
            quintile_latest_grp_1['TRIM_SKU']=quintile_latest_grp_1['TRIM_SKU'].astype(str)
            quintile_latest_grp_1 = pd.merge(quintile_latest_grp_1, db_dim_sku[intesection(match_col, df_inv_map.columns)+['SKU_SEQ_ID']], on=intesection(match_col, df_inv_map.columns), how='left')
            
            quintile_latest_grp_1 = quintile_latest_grp_1[intesection(quintile_latest_grp_1.columns,db_table1.iloc[:,0].tolist())]
            quintile_latest_grp_1['DEAL_ID']=deal_id
            quintile_latest_grp_1['QUINTILE']=quintile_latest_grp_1['QUINTILE'].astype(str)
            s1 = time.time()
            #success, nchunks, nrows, _ = write_pandas(ctx, renaming(quintile_latest_grp_1), f'FACT_{dict_month[month]}_MONTH_QUINTILE')
            s2 = time.time()
            total_insert_time = total_insert_time + (s2-s1)
        # quintile_latest_grp_1[quintile_latest_grp_1['SKU_SEQ_ID'].isna()]['TRIM_SKU']
        # db_dim_sku[db_dim_sku['TRIM_SKU'].isin(['13613','13615'])]
        # quintile_latest_grp_1.to_excel('2m_quintile_10795.xlsx')
        
        
        print('''###############################################
        
        '7. Inventory Movement'
        
        ###############################################''')
        match_col = ['LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE', 'TRIM_SKU']
        inv_mvmt = pd.merge(df_inv_map, db_dim_sku, on=intesection(
            match_col, df_sales_map.columns), how='left')
        fact_inv_db['INVENTORY_DATE'] = pd.to_datetime(fact_inv_db['INVENTORY_DATE'])
        fact_inv_db['BOM_INV_DATE'] = fact_inv_db['INVENTORY_DATE'].apply(
            lambda x: x.replace(day=1) if pd.notnull(x) else x)
        
        
        fact_inv_db_cpy = fact_inv_db.groupby(["BOM_INV_DATE", "SKU_SEQ_ID"]).agg("sum").reset_index().copy()
        fact_inv = fact_inv_db_cpy.copy()
        
        SIX_OR_TWELVE = 12
        
        for i in range(SIX_OR_TWELVE):
            fact_inv[str(i)+"_month"] = fact_inv.BOM_INV_DATE - pd.DateOffset(months = i)
        
        fact_inv = fact_inv[["SKU_SEQ_ID","BOM_INV_DATE"]+[str(i)+"_month" for i in range(SIX_OR_TWELVE)]]
        fact_inv.rename({"BOM_INV_DATE":"OG"},inplace = True, axis =1)
        fact_inv.set_index(["SKU_SEQ_ID", "OG"], inplace = True)
        fact_inv = fact_inv.stack()
        fact_inv = fact_inv.reset_index()
        fact_inv.rename({"level_2":"MONTH", 0:"BOM_INV_DATE"}, inplace = True, axis=  1)
        fact_inv = pd.merge(fact_inv, fact_inv_db_cpy[["SKU_SEQ_ID","BOM_INV_DATE", "QUANTITY_ON_HAND"]], how = "left", on = ["SKU_SEQ_ID","BOM_INV_DATE"])
        
        temp_pivot = pd.pivot_table(fact_inv,values = "QUANTITY_ON_HAND", index = ["SKU_SEQ_ID","OG"], columns = "MONTH")
         
        temp_pivot = temp_pivot.reset_index().fillna(0)
        
        print(temp_pivot.columns)
        
        dummy = -temp_pivot.loc[:,[str(i) + "_month" for i in range(1,SIX_OR_TWELVE)]].values + temp_pivot.loc[:,[str(i) + "_month" for i in range(0,SIX_OR_TWELVE-1)]].values
        
        dummy = pd.DataFrame(dummy)
        dummy1 = dummy.copy()
        dummy = dummy.mask(dummy1 < 0, 'Down')    #check
        dummy = dummy.mask(dummy1 > 0, 'Up')
        dummy = dummy.mask(dummy1 == 0, 'Static')
        dummy = dummy.mask(dummy1.isna(), 'No Inv')
        
        dummy = dummy.mask(temp_pivot.loc[:,[str(i) + "_month" for i in range(0,SIX_OR_TWELVE-1)]].values==temp_pivot.loc[:,[str(i) + "_month" for i in range(1,SIX_OR_TWELVE)]].values,"No Inv")
        
        dummy["SKU_SEQ_ID"] = temp_pivot["SKU_SEQ_ID"].copy()
        dummy["OG"] = temp_pivot["OG"].copy()
        
        temp_pivot = pd.merge(dummy, temp_pivot, how = "right", on = ["SKU_SEQ_ID","OG"] )
        
        
        def count_upsanddowns(df, arg, num_months):
            k = list(range(0, num_months))
            return (df.loc[:,k] == arg).sum(axis = 1)
        
        
        for arg in ["Up", "Down", "No Inv", "Static"]:
            temp_pivot[("TWELVE_MONTH_"+arg).upper().replace(" ","_")] = count_upsanddowns(temp_pivot, arg, 11)
            
        
        for arg in ["Up", "Down", "No Inv", "Static"]:
            temp_pivot[("SIX_MONTH_"+arg).upper().replace(" ","_")] = count_upsanddowns(temp_pivot, arg, 5)
            
        def mvmt_cat(df,col):
            suffix = "_".join(col.split("_")[0:2])
            df.loc[(df[(suffix+'_Down').upper()]==0)&(df[(suffix+'_Up').upper()]==0),col]='Static'
            df.loc[(df[(suffix+'_Down').upper()]==0)&(df[(suffix+'_Up').upper()]!=0),col]='Up'
            df.loc[(df[(suffix+'_Down').upper()]!=0)&(df[(suffix+'_Up').upper()]==0),col]='Down'
            df.loc[(df[(suffix+'_Down').upper()]!=0)&(df[(suffix+'_Up').upper()]!=0),col]='Up/Down'
            return df
        
        temp_pivot = mvmt_cat(temp_pivot, "SIX_MONTH_MVMT_CAT")
        temp_pivot = mvmt_cat(temp_pivot, "TWELVE_MONTH_MVMT_CAT")
            
        temp_pivot_cpy = temp_pivot.copy()
        
        
        
        for i in range(SIX_OR_TWELVE-1):
            ind = temp_pivot_cpy.loc[(temp_pivot_cpy.loc[:,i]=='Down')^(temp_pivot_cpy.loc[:,i]=='Up'),i].index
            temp_pivot_cpy=temp_pivot_cpy.drop(ind)
            temp_pivot.loc[ind,'MONTHS_LAST_MOVE']=i
            temp_pivot.loc[ind, "LAST_MOVE_DATE"] = temp_pivot.loc[ind, "OG"] - pd.DateOffset(months = i)
            temp_pivot.loc[ind, "LAST_MOVE_DAYS"] = (pd.to_datetime(temp_pivot.loc[ind, "OG"]) - pd.to_datetime(temp_pivot.loc[ind, "LAST_MOVE_DATE"])).dt.days
        
        temp_pivot.loc[temp_pivot['MONTHS_LAST_MOVE'].isna(),'MOST_LAST_MOVE']=0
        
        date_key = pd.read_excel('Buckets_Range.xlsx')
        def lm_buckets(bucket, col1, col2):
            for i in range(len(date_key)):
                lb = date_key.loc[i, 'Lower Bound']
                ub = date_key.loc[i, 'Upper Bound']
        
                bucket.loc[bucket[col1].between(lb, ub), col2] = date_key.loc[i, 'Value']
            return bucket
        
        temp_pivot=lm_buckets(temp_pivot,'LAST_MOVE_DAYS','LASTMOVE_BUCKET_SEQ_ID')
        temp_pivot.rename({"OG":"BOM_INV_DATE"}, axis=1, inplace = True)
        fact_inv_db_cpy = pd.merge(fact_inv_db_cpy, temp_pivot, how = "left", on = ["SKU_SEQ_ID","BOM_INV_DATE"])
        fact_inv_db_cpy.rename(columns={'TOTAL_COST_EXTENDED':'TOTAL_COST','QUANTITY_ON_HAND':'TOTAL_QUANTITY'},inplace=True)
        fact_inv_db_cpy['CPU']=fact_inv_db_cpy['TOTAL_COST']/fact_inv_db_cpy['TOTAL_QUANTITY']
        
        fact_inv_db_cpy['DATE_KEY']=fact_inv_db_cpy['BOM_INV_DATE'].dt.date.astype(str).str.replace('-','')
        
        
        db_table = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                     sheet_name='FACT_INV_MVMT')
        
        df_mvt = fact_inv_db_cpy[intesection(fact_inv_db_cpy.columns,db_table.iloc[:,1].tolist())]
        df_mvt.dropna(axis=1, how='all',inplace=True)
        df_mvt.replace('nan',0, inplace=True)
        df_mvt.replace(np.nan,0, inplace=True)
        df_mvt.rename(columns={'FIRST_SALE_DATE':'FIRST_SALES_DATE'},inplace=True)
        
        df_mvt.loc[np.abs(df_mvt['CPU'])==np.inf,'CPU']=0
        df_mvt.loc[df_mvt.LAST_MOVE_DATE==0, "LAST_MOVE_DATE"]= datetime(1900, 1, 1)
        df_mvt.LAST_MOVE_DATE = pd.to_datetime(df_mvt.LAST_MOVE_DATE).dt.date
        df_mvt.loc[:,'INSERT_DATE'] = datetime.now().date()
        
        s1  = time.time()
        
        #success, nchunks, nrows, _ = write_pandas(ctx, renaming(df_mvt), 'FACT_INV_MVMT')
        s2  = time.time()
        total_insert_time = total_insert_time + (s2-s1)
        
        print('''###############################################
        
        '8. FIFO'
        
        ###############################################''')
        from tqdm import tqdm
        #df_sales_map = df_sales_map.rename({'TRIM SKU':'TRIM_SKU'},axis=1)
        #df_inv_map = df_inv_map.rename({'TRIM SKU':'TRIM_SKU'},axis=1)
        #db_dim_sku = df_sales_map[intesection(match_col, df_inv_map.columns)+['BOM']].append(df_inv_map[intesection(match_col, df_inv_map.columns)+['BOM']]).groupby(intesection(match_col, df_inv_map.columns))['BOM'].min().reset_index()
        #db_dim_sku['SKU_SEQ_ID'] = np.random.randint(1000000000,100000000000, len(db_dim_sku), dtype=np.int64)
        #df_sales_map['BOM'] = df_sales_map['BOM'].dt.date
        #df_inv_map['BOM'] = df_inv_map['BOM'].dt.date
        
        df_inv_map_copy = df_inv_map.copy()
        df_sales_map_copy = df_sales_map.copy()
        
        df_inv_map = df_inv_map.groupby(intesection(match_col, df_inv_map.columns)+['BOM_INV_DATE'])['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED'].agg('sum').reset_index()
        df_sales_map = df_sales_map.groupby(intesection(match_col, df_inv_map.columns)+['BEG_MO'])['QUANTITY_SOLD','NET_SALES_$','COGS_$','GROSS_MARGIN_$'].agg('sum').reset_index()
        #df_sales_map['SKU_SEQ_ID'] = np.random.randint(1000000000,100000000000, len(df_sales_map), dtype=np.int64)
        
        df_inv_map = df_inv_map.rename({'BOM_INV_DATE':'BOM'}, axis=1)
        df_sales_map = df_sales_map.rename({'BEG_MO':'BOM'}, axis=1)
        
        #CHECKING IF NEED TO CALCULATE FIFO OR NOT
        # CASES WHERE WE DON'T CALCULATE FIFO AT ALL:
            #1. IF NO OF UNIQUE BOM_INV_DATES IS LESS THAN 12
            #2. IF LATEST 12 MONTHS HAVE GAPS IN BETWEEN(NOT CONTINUOUS)
        
        bom_inv_dates = pd.DataFrame()
        TEMP = df_inv_map.BOM.unique()
        bom_inv_dates["bom_inv_dates"] = TEMP
        bom_inv_dates= bom_inv_dates.sort_values(by = "bom_inv_dates",ascending=False)
        
        
        if bom_inv_dates.shape[0]<12:
            CONDITION = False
        else:
            months_unique_latest = [max(bom_inv_dates["bom_inv_dates"])+dateutil.relativedelta.relativedelta(months=-i) for i in range(12)]
            CONDITION = True
            for d,d_ in zip(bom_inv_dates.loc[:12, "bom_inv_dates"].tolist(), months_unique_latest):
                if d != d_:
                    CONDITION = False
                    break
        
                
        if CONDITION:
            
            mapped_df = pd.merge(df_sales_map, df_inv_map, how='right', on=intesection(match_col, df_inv_map.columns)+['BOM']) #here we get data from both sales and inv
            #db_table = pd.read_excel('Test Python Schema Tables_updated.xlsx',
            #                             sheet_name='FIFO_M1')   #change made by manoj
            
        
            
            db_table = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                         sheet_name='FIFO_M1') 
            
            #mapped_df.to_excel('mapped_df.xlsx',index=False)
            
            #get max BOM date
            max_bom_date = max(mapped_df['BOM'])
            print("max date: ",max_bom_date)
            
            cols = []
            for i in range(25):
                cols.append(f'{i*30}_{(i+1)*30}')
            from tqdm import tqdm
            special_cols = ["90_180",	"180_270",	"270_360",	"361_540",	"541_720"] 
            #list_df = []
            # taking prev_df and curr_df for calc
            #db_dim_sku.loc[db_dim_sku['SKU_SEQ_ID']==32160539246,'TRIM_SKU']
            #t = df_inv_map[df_inv_map['TRIM_SKU']=='BOWERY414']
            for i in tqdm(range(0,24)): #change made by manoj
          
                prev_date = max_bom_date - dateutil.relativedelta.relativedelta(months=24-i)  #change made by manoj
                print("prev date: ",prev_date)
                curr_date = max_bom_date - dateutil.relativedelta.relativedelta(months=23-i)  #change made by manoj
                print("curr date: ",curr_date)
                
                
            
            #    hist_date = []
            #    for k in range(0,25):
            #        hist_date.append((curr_date-dateutil.relativedelta.relativedelta(months=k)))
                
                prev_df = mapped_df[(mapped_df['BOM'] == prev_date)]
                prev_df = prev_df.groupby(intesection(match_col, df_inv_map.columns))['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED','QUANTITY_SOLD','NET_SALES_$','COGS_$','GROSS_MARGIN_$'].agg('sum').reset_index()
                
                curr_df = mapped_df[(mapped_df['BOM'] == curr_date)]
                curr_df = curr_df.groupby(intesection(match_col, df_inv_map.columns))['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED','QUANTITY_SOLD','NET_SALES_$','COGS_$','GROSS_MARGIN_$'].agg('sum').reset_index()
        
            #    curr_df['CURRENT_UNITS'] = curr_df['QUANTITY_ON_HAND_y'].tolist()
            #    curr_df['CURRENT_COST'] = curr_df['TOTAL_COST_EXTENDED_y'].tolist()
                
                if (len(prev_df)==0) and (len(curr_df)!=0):
                    merged_df = curr_df.copy()
                    ### '_x' is previous and '_y' is current
                    merged_df['QUANTITY_ON_HAND_x'], merged_df['TOTAL_COST_EXTENDED_x'], merged_df['QUANTITY_SOLD_x'], merged_df['NET_SALES_$_x'], merged_df['COGS_$_x'], merged_df['GROSS_MARGIN_$_x'] = [0,0,0,0,0,0] 
                    
                    merged_df[['QUANTITY_ON_HAND_y','TOTAL_COST_EXTENDED_y','QUANTITY_SOLD_y','NET_SALES_$_y','COGS_$_y','GROSS_MARGIN_$_y']] = curr_df[['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED','QUANTITY_SOLD','NET_SALES_$','COGS_$','GROSS_MARGIN_$']]
                    merged_df.drop(['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED','QUANTITY_SOLD','NET_SALES_$','COGS_$','GROSS_MARGIN_$'], axis=1, inplace=True)
                elif (len(prev_df)!=0) and (len(curr_df)!=0):
                    merged_df = pd.merge(prev_df, curr_df, how='right', on=intesection(match_col, df_inv_map.columns))
                
                elif (len(prev_df)==0) and (len(curr_df)==0):
                    continue  
                  
                else:
                    
                    continue
        
                ############################# CALCULATIONS #########################################
                merged_df['CURRENT_UNITS'] = merged_df['QUANTITY_ON_HAND_y'].tolist()
                merged_df['CURRENT_COST'] = merged_df['TOTAL_COST_EXTENDED_y'].tolist()
                
                merged_df['QUANTITY_ON_HAND_x'] = merged_df['QUANTITY_ON_HAND_x'].fillna(0)
                merged_df['CURRENT_UNITS'] = merged_df['CURRENT_UNITS'].fillna(0)
                merged_df['QUANTITY_SOLD_y'] = merged_df['QUANTITY_SOLD_y'].fillna(0)
                #merged_df.loc[merged_df["QUANTITY_ON_HAND_x"] == 0, "QUANTITY_SOLD_y"] = 0
            #    merged_df.to_excel('merged_df_'+period+'.xlsx',index=False)
            
                #EOM CHANGE
                merged_df['eom_change'] = merged_df['CURRENT_UNITS'] - merged_df['QUANTITY_ON_HAND_x']  
            
                #SALES ADJ
                merged_df.loc[(merged_df['QUANTITY_SOLD_y']>=0), 'sales_adj'] = merged_df['QUANTITY_SOLD_y']
                merged_df.loc[(merged_df['QUANTITY_SOLD_y']<0) & (merged_df['eom_change']>=0), 'sales_adj'] = merged_df['eom_change']   
                merged_df.loc[(merged_df['QUANTITY_SOLD_y']<0) & (merged_df['eom_change']<0), 'sales_adj'] = 0
                
                #RECEIVED ADJ
               # merged_df.loc[(merged_df['sales_adj']>0), 'rec_adj'] = merged_df['sales_adj']+merged_df['eom_change']
                merged_df.loc[:, 'rec_adj'] = merged_df['sales_adj']+merged_df['eom_change']   #change made by manoj
                merged_df.loc[(merged_df['eom_change']==0) & (merged_df['sales_adj']<0), 'rec_adj'] = 0
                merged_df.loc[(merged_df['sales_adj']==0) & (merged_df['eom_change']!=0), 'rec_adj'] = merged_df['eom_change']  
                
                #USE SALES
               # merged_df.loc[(merged_df['rec_adj']>=0) & (merged_df['CURRENT_UNITS']!=0) & (merged_df['sales_adj']>=0), 'use_sales'] = merged_df['sales_adj']
                merged_df.loc[:, 'use_sales'] = merged_df['sales_adj']
                merged_df.loc[(merged_df['sales_adj']<0), 'use_sales'] = merged_df['sales_adj']*-1 + merged_df['sales_adj']    
                merged_df.loc[(merged_df['CURRENT_UNITS']==0), 'use_sales'] = merged_df['eom_change']*-1
                merged_df.loc[(merged_df['rec_adj']<0), 'use_sales'] = merged_df['eom_change']*-1
                
                #USE RECEIPTS
                # merged_df.loc[(merged_df['rec_adj']>=0), 'use_receipts'] = merged_df['rec_adj']
                merged_df.loc[:, 'use_receipts'] = merged_df['rec_adj']
                merged_df.loc[(merged_df['rec_adj']<0), 'use_receipts'] = 0
                merged_df.loc[(merged_df['CURRENT_UNITS']==0) & (merged_df['use_sales']==-merged_df['eom_change']), 'use_receipts'] = 0
            
                #CPU
                merged_df['CURRENT_CPU'] = merged_df['CURRENT_COST']/merged_df['CURRENT_UNITS']
                    
                merged_df['SP_U'] = merged_df['NET_SALES_$_y']/merged_df['use_sales']
                merged_df['GM_U'] = merged_df['GROSS_MARGIN_$_y']/merged_df['use_sales']
                
                df_to_be_merged = db_dim_sku[intesection(match_col, df_inv_map.columns)+['SKU_SEQ_ID']]
                df_to_be_merged = df_to_be_merged.loc[df_to_be_merged.TRIM_SKU.isin(list(set(df_inv_map.TRIM_SKU))), :]
                
                merged_df= pd.merge(merged_df, df_to_be_merged, on=intesection(match_col, df_inv_map.columns), how='right')   #changed right to left
            
                if i==0: # tricky part
                    
                    m0_aging = pd.DataFrame(0, index=np.arange(len(merged_df)), columns=['UNITS_'+i for i in cols], dtype=object)
                    m0_aging['SKU_SEQ_ID'] = merged_df['SKU_SEQ_ID'].copy()
                    m0_aging['use_receipts'] = merged_df['use_receipts'].copy()
                    
                    temp_aging = m0_aging.copy()
                    
            #        m0_su = pd.DataFrame(0, index=np.arange(len(merged_df)), columns=[i+'_sales_unit' for i in cols], dtype=object)
            #        m0_su['SKU_SEQ_ID'] = merged_df['SKU_SEQ_ID'].copy()
            #        
            #        temp_su = m0_su.copy()
                
                sum1 = pd.DataFrame()
                sum1['sum'] = [0]*len(merged_df)
                cumu = []
                
                temp_aging.fillna(0, inplace = True)
                
                for j in reversed(cols):
            #        print (j)
                    cumu.append('UNITS_'+j)
                    
                    if len(cumu)>1:
                        
                        merged_df.loc[merged_df['use_sales'].abs() > np.nansum(temp_aging[cumu], axis=1), 'A_'+str(j)+'_ASQ'] = temp_aging['UNITS_'+j]
                        merged_df.loc[merged_df['use_sales'].abs() <= np.nansum(temp_aging[cumu], axis=1),'A_'+str(j)+'_ASQ'] = (merged_df['use_sales']-sum1['sum']).clip(lower=0)
                       
                        sum1['sum'] = sum1['sum'] + temp_aging["UNITS_"+j] #change made by manoj
                        
                    else:
                        merged_df.loc[merged_df['use_sales'].abs() > np.nansum(temp_aging[cumu], axis=1), 'A_'+str(j)+'_ASQ'] = temp_aging['UNITS_'+j]
                        merged_df.loc[merged_df['use_sales'].abs() <= np.nansum(temp_aging[cumu], axis=1), 'A_'+str(j)+'_ASQ'] = merged_df['use_sales']
                        sum1['sum'] = sum1['sum'] + temp_aging["UNITS_"+j]   #change made by manoj
                
            #    temp_su = merged_df[[i for i in merged_df.columns if '_sales_unit' in i]]
            #    temp_su['SKU_SEQ_ID'] = merged_df['SKU_SEQ_ID'].copy()
                
            #    merged_df[merged_df[[i for i in merged_df.columns if '_sales_unit' in i]]<0]
                merged_df['total_receipts'] = merged_df['use_sales'] - np.nansum(merged_df[[i for i in merged_df.columns if '_ASQ' in i]], axis=1)
            
                curr_su = pd.DataFrame(columns = ['total_receipts']+[i for i in merged_df.columns if '_ASQ' in i], dtype=object)
                curr_su['total_receipts'] = merged_df['total_receipts'].copy()
                curr_su[[i for i in merged_df.columns if '_ASQ' in i]] = merged_df[[i for i in merged_df.columns if '_ASQ' in i]].copy()
            
                
              
                curr_su_use = pd.concat([curr_su.iloc[:,0:1], curr_su.iloc[:,2:].iloc[:, ::-1]], axis=1) 
                temp_aging_use = pd.concat([merged_df.loc[:,'use_receipts'], temp_aging.iloc[:,:-3]], axis=1)  
                
                
                
                aging = temp_aging_use.values - curr_su_use.values
                
                # merged_df[[i for i in cols]] = aging
                #merged_df.to_excel(f"ultimate_{i}.xlsx")
               
                
                df_aging = pd.DataFrame(aging, columns=['UNITS_'+i for i in cols], dtype=object)
                df_aging['SKU_SEQ_ID'] = merged_df['SKU_SEQ_ID'].copy()
                df_aging['use_receipts'] = merged_df['use_receipts'].copy()
        
                
                df_aging_cost = df_aging.copy()
                df_aging_cost = df_aging_cost.mul(merged_df['CURRENT_CPU'],axis=0)
                df_aging_cost.columns = [i.replace("UNITS","COST") for i in df_aging_cost.columns]
                
                df_sales_cost = merged_df[[i for i in merged_df.columns if '_ASQ' in i]].copy()
                df_sales_cost=df_sales_cost.mul(merged_df['SP_U'],axis=0)
                df_gross_margin_cost = merged_df[[i for i in merged_df.columns if '_ASQ' in i]].mul(merged_df['GM_U'],axis=0)
                
                df_sales_cost.columns = [i.replace('_ASQ','')+'_S$A' for i in df_sales_cost.columns]
                df_gross_margin_cost.columns = [i.replace('_ASQ','')+'GM$A' for i in df_gross_margin_cost.columns]
                
                temp_aging = df_aging.copy()
                
                merged_df = pd.concat([merged_df, df_aging, df_aging_cost, df_sales_cost, df_gross_margin_cost], axis=1)
                
                db_fifo = merged_df[intesection(db_table.iloc[:,1], merged_df.columns)]
              
                db_fifo = db_fifo.loc[:, ~db_fifo.columns.duplicated()]
                db_fifo.replace([-np.inf,np.inf], np.nan, inplace=True)
                
                for col in special_cols:
                    l, u = col.split("_")
                    l,u = int(l), int(u)
                    l, u  = (l//10)*10 , (u//10)*10
                    cols_to_be_added = list(range(l,u, 30))
                    cols_to_be_added = ["UNITS_"+str(l)+"_"+str(l+30) for l in cols_to_be_added]
                    cpy = db_fifo[cols_to_be_added].fillna(0)
                    db_fifo["UNITS_"+col] = cpy.sum(axis = 1)
                    
                db_fifo["UNITS_OVER_720"] = db_fifo["UNITS_720_750"].copy()
                    
                for col in special_cols:
                    l, u = col.split("_")
                    l,u = int(l), int(u)
                    l, u  = (l//10)*10 , (u//10)*10
                    cols_to_be_added = list(range(l,u, 30))
                    cols_to_be_added = ["COST_"+str(l)+"_"+str(l+30) for l in cols_to_be_added]
                    cpy = db_fifo[cols_to_be_added].fillna(0)
                    db_fifo["COST_"+col] = cpy.sum(axis=1)
                    
                db_fifo["COST_OVER_720"] = db_fifo["COST_720_750"].copy()
                
                db_fifo["DEAL_ID"] = deal_id
                db_fifo["INSERT_DATE"] = datetime.now().date()
                
                
                
                db_fifo = pd.merge(db_fifo, db_dim_sku[["COUNTRY", "DIVISION", "INVENTORY_TYPE", "SKU_SEQ_ID"]], how = "left", on = ["SKU_SEQ_ID"])
                
                db_fifo["DATE_KEY"] = str(curr_date.date()).replace("-","")
         
                s1 = time.time()
                #success, nchunks, nrows, _ = write_pandas(ctx, renaming(db_fifo), f'FIFO_M{i}', parallel=99)
                s2= time.time()
                total_insert_time = total_insert_time + (s2-s1)
                
            
            
        print('''###############################################
        
        '9. GROSS_MARGIN_CYCLE'
        
        ###############################################''')
        
        
        start = time.time()
        db_table = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                     sheet_name='FACT_GROSS_MARGIN_CYCLE')
        
        match_col = ['LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE','TRIM_SKU', "DEAL_ID"] #DEAL_ID is added by manoj
        df_sales_map_copy = df_sales_map_copy.rename({'BOM':'BEG_MO'},axis=1)
        df_inv_map_copy = df_inv_map_copy.rename({'BOM':'BOM_INV_DATE'},axis=1)
        #create measure_sale groupby match_col and BEG_MO column
        
        
        
        df_sales_map_copy_columns= list(measure_sale.columns)
        if "GROSS_MARGIN_$" not in df_sales_map_copy_columns and "COGS_$" not in df_sales_map_copy_columns:
            df_sales_map_copy["GROSS_MARGIN_$"] = 0    
            df_sales_map_copy["COGS_$"] = 0
        elif "GROSS_MARGIN_$" not in df_sales_map_copy_columns:
            df_sales_map_copy["GROSS_MARGIN_$"] = df_sales_map_copy["NET_SALES_$"] - df_sales_map_copy["COGS_$"]
        elif "COGS_$" not in df_sales_map_copy_columns:
            df_sales_map_copy["COGS_$"] = df_sales_map_copy["NET_SALES_$"] - df_sales_map_copy["GROSS_MARGIN_$"]
        else:
            pass
        
        
        
        measure_sale = df_sales_map_copy.fillna('NA').groupby(intesection(match_col, df_sales_map_copy.columns) + ['BEG_MO'])[intesection(['GROSS_MARGIN_$'], df_sales_map_copy.columns)].sum().reset_index()
        #get first_sale_date
        first_sale = measure_sale.fillna('NA').groupby(intesection(match_col, df_sales_map_copy.columns))['BEG_MO'].min().reset_index()
        first_sale = first_sale.rename({'BEG_MO':'FIRST_SALE_DATE'},axis=1)
        #get last_sales_date
        last_sale = measure_sale.fillna('NA').groupby(intesection(match_col, df_sales_map_copy.columns))['BEG_MO'].max().reset_index()
        last_sale = last_sale.rename({'BEG_MO':'LAST_SALES_DATE'},axis=1)
        #get first_receipt_date
        first_receipt = df_inv_map_copy.fillna('NA').groupby(intesection(match_col, df_inv_map_copy.columns))['BOM_INV_DATE'].min().reset_index()
        first_receipt = first_receipt.rename({'BOM_INV_DATE':'FIRST_RECEIPT_DATE'},axis=1)
        
        last_receipt = df_inv_map_copy.fillna('NA').groupby(intesection(match_col, df_inv_map_copy.columns))['BOM_INV_DATE'].max().reset_index()
        last_receipt = last_receipt.rename({'BOM_INV_DATE':'LAST_RECEIPT_DATE'},axis=1)
        
        #merge measure_sale and first_sale_date
        measure_sale = pd.merge(measure_sale, first_sale, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        #merge measure_sale and last_sales_date
        measure_sale = pd.merge(measure_sale, last_sale, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        #merge measure_sale and first_receipt_date
        measure_sale = pd.merge(measure_sale, first_receipt, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        
        measure_sale = pd.merge(measure_sale, last_receipt, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        
        #create introduction_date
        measure_sale['INTRODUCTION_DATE'] = measure_sale[['FIRST_SALE_DATE','FIRST_RECEIPT_DATE']].min(axis=1)  ## use new function
        
        
        
        
        
        measure_sale['GROSS_MARGIN_$'] = measure_sale['GROSS_MARGIN_$'].fillna(0)
        measure_sale[intesection(match_col, df_sales_map_copy.columns)] = measure_sale[intesection(match_col, df_sales_map_copy.columns)].fillna('NA')
        
        measure_sale['BEG_MO'] = measure_sale['BEG_MO'].dt.date
        
        measure_sale = temp_bucket(measure_sale,'sale',deal_id)
        ############################
        
        measure_sale=pd.merge(measure_sale,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']],on=intesection(match_col, df_sales_map_copy.columns),how='left').drop_duplicates()
        
        # measure_sale.columns
        test24=measure_sale.loc[:,['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MO']]
        test24.columns=['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MON']
        for i in range(24):
            test24['GROSS_MARGIN_CYCLE_'+str(i+1).zfill(2)] = test24.FIRST_SALE_DATE + pd.DateOffset(months = i)
        test24.set_index(['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MON'],inplace=True)
        test24=test24.stack()
        test24=test24.reset_index()
        test24.columns=['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MON','MONTH','BEG_MO']
        
        # test24=pd.merge(test24,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']],on=intesection(match_col, df_sales_map_copy.columns),how='left').drop_duplicates()
        fact_sale_db_copy=fact_sale_db.copy()
        fact_sale_db_copy['BEG_MO']=pd.to_datetime(fact_sale_db_copy['BEG_MO'])
        test24['BEG_MO']=pd.to_datetime(test24['BEG_MO'])
        test24.set_index(['SKU_SEQ_ID','BEG_MO'],inplace=True)
        fact_sale_db_copy.set_index(['SKU_SEQ_ID','BEG_MO'],inplace=True)
        test24['GROSS_MARGIN_$']=fact_sale_db_copy['GROSS_MARGIN_$']
        test24=test24.reset_index()
        final=pd.pivot_table(test24, values='GROSS_MARGIN_$', columns='MONTH', index=['SKU_SEQ_ID','BEG_MON'], aggfunc=np.sum).reset_index()
        final.rename(columns={'BEG_MON':'BEG_MO'},inplace=True)
        
        measure_sale['BEG_MO']=pd.to_datetime(measure_sale['BEG_MO'])
        final['BEG_MO']=pd.to_datetime(final['BEG_MO'])
        
        measure_sale=pd.merge(measure_sale,final,on=['SKU_SEQ_ID','BEG_MO'],how='left')
        measure_sale['BEG_MO']=pd.to_datetime(measure_sale['BEG_MO'])
        
        measure_sale['INSERT_DATE'] = datetime.now().date()
        measure_sale['DATE_KEY'] = measure_sale['BEG_MO'].dt.date.astype(str).str.replace('-','')
        measure_sale = measure_sale.applymap(
                lambda x: x.date() if isinstance(x, datetime) else x)
        
        # for i in measure_sale.columns:
        #     if 'month' in i:
        #         print(i)
        # for i in range(24):
        #     print('GROSS_MARGIN_CYCLE_'+str(i+1).zfill(2))
        
        ############################33
        # fact_sale_db.groupby(intesection(match_col, df_sales_map_copy.columns))[['GROSS_MARGIN_$']].sum()
        # test5=pd.merge(measure_sale,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']],on=intesection(match_col, df_sales_map_copy.columns),how='left')
        # pd.date_range(measure_sale['FIRST_SALE_DATE'], freq="M", periods=24)
        # measure_sale['FIRST_SALE_DATE']+dateutil.relativedelta.relativedelta(24)
        # measure_sale['FIRST_SALE_DATE'] + pd.DateOffset(months=24)
        ################################## new logic
        # test24=measure_sale.loc[:,intesection(match_col, df_sales_map_copy.columns)+['FIRST_SALE_DATE','BEG_MO']]
        # test24.columns=intesection(match_col, df_sales_map_copy.columns)+['FIRST_SALE_DATE','BEG_MON']
        # for i in range(24):
        #     test24[str(i+1)+"_month"] = test24.FIRST_SALE_DATE + pd.DateOffset(months = i)
            
        # test24.set_index(intesection(match_col, df_sales_map_copy.columns)+['FIRST_SALE_DATE','BEG_MON'],inplace=True)
        # test24=test24.stack()
        # test24=test24.reset_index()
        # test24.columns=intesection(match_col, df_sales_map_copy.columns)+['FIRST_SALE_DATE','BEG_MON']+['MONTH','BEG_MO']
        
        # test24=pd.merge(test24,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']],on=intesection(match_col, df_sales_map_copy.columns),how='left').drop_duplicates()
        # fact_sale_db_copy=fact_sale_db.copy()
        # fact_sale_db_copy['BEG_MO']=pd.to_datetime(fact_sale_db_copy['BEG_MO'])
        # test24['BEG_MO']=pd.to_datetime(test24['BEG_MO'])
        # test24.set_index(['SKU_SEQ_ID','BEG_MO'],inplace=True)
        # fact_sale_db_copy.set_index(['SKU_SEQ_ID','BEG_MO'],inplace=True)
        # test24['GROSS_MARGIN_$']=fact_sale_db_copy['GROSS_MARGIN_$']
        # test24=test24.reset_index()
        # final=pd.pivot_table(test24, values='GROSS_MARGIN_$', columns='MONTH', index=intesection(match_col, df_sales_map_copy.columns)+['BEG_MON'], aggfunc=np.sum).reset_index()
        ####################################
        # test1=pd.merge(test24,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID','GROSS_MARGIN_$']],on=intesection(match_col, df_sales_map_copy.columns),how='left').drop_duplicates(subset=intesection(match_col, df_sales_map_copy.columns)+['BEG_MO'],keep='first')
        
        # test24 = pd.melt(test24, id_vars='SKU_SEQ_ID', value_vars=[str(i+1)+'_month' for i in range(24)],
        #               var_name='MONTH', value_name='BEG_MO')
        
        
        
        # pd.merge(test24,fact_sale_db[['SKU_SEQ_ID','BEG_MO','GROSS_MARGIN_$']],on=['SKU_SEQ_ID','BEG_MO'],how='left')
        
        # test24.columns=intesection(match_col, df_sales_map_copy.columns)+['FIRST_SALE_DATE','BEG_MON']+['MONTH','BEG_MO']
        # test24.set_index(intesection(match_col, df_sales_map_copy.columns),inplace=True)
        # fact_sale_db_copy=fact_sale_db.copy()
        # fact_sale_db_copy.set_index(intesection(match_col, df_sales_map_copy.columns),inplace=True)
        # test24.loc[:,'SKU_SEQ_ID']=fact_sale_db_copy['SKU_SEQ_ID']
        # # test4 = pd.melt(test24, id_vars=intesection(match_col, df_sales_map_copy.columns), value_vars=[str(i+1)+'_month' for i in range(24)],
        # #               var_name='MONTH', value_name='BEG_MO_x')
        # test24.drop('FIRST_SALE_DATE',axis=1,inplace=True)
        
        # test24['BEG_MO']=pd.to_datetime(test24['BEG_MO'])
        # fact_sale_db_copy['BEG_MO']=pd.to_datetime(fact_sale_db_copy['BEG_MO'])
        # test24=pd.merge(test24,fact_sale_db_copy[['GROSS_MARGIN_$','SKU_SEQ_ID','BEG_MO']],on=['SKU_SEQ_ID','BEG_MO'],how='left')
        # test24.sort_values(['SKU_SEQ_ID','BEG_MO','MONTH'])
        # test24.drop('FIRST_SALE_DATE',axis=1,inplace=True)
        # test24_cpy=test24.stack()
        # test24_cpy=test24_cpy.reset_index()
        # test24_cpy.columns=intesection(match_col, df_sales_map_copy.columns)+['MONTH','BEG_MO']
        # test24_cpy['TRIM_SKU']=test24_cpy['TRIM_SKU'].astype(str)
        # test1=pd.merge(test24_cpy,df_sales_map_copy[intesection(match_col, df_sales_map_copy.columns)+['BEG_MO','GROSS_MARGIN_$']],on=intesection(match_col, df_sales_map_copy.columns)+['BEG_MO'],how='left')
        
        # test24['Next_24']=test24['FIRST_SALE_DATE']+pd.DateOffset(months=24)
        # pd.DataFrame(index=range(len(measure_sale)),columns=[str(i)+'_month' for i in range(24)]).s
        # test24.dtypes
        ###########################################
        # start_date = measure_sale['BEG_MO'].min()  #####comment
        
        # str_res_max = measure_sale['BEG_MO'].apply(lambda x: str(x).replace('-',''))#change made by manoj
        # str_res_max = str(measure_sale['BEG_MO']).replace('-','')
        # measure_sale['BEG_MO'].dt.date.apply(lambda x: str(x).replace('-',''))
        # res = max(measure_sale['BEG_MO']) #####comment
        
        # final_df = pd.pivot_table(measure_sale, values='GROSS_MARGIN_$', columns='BEG_MO', index=intesection(match_col, df_sales_map_copy.columns)+ ['FIRST_SALE_DATE','FIRST_RECEIPT_DATE','INTRODUCTION_DATE','LAST_SALES_DATE','LAST_RECEIPT_DATE']+[i for i in measure_sale if 'bucket' in i.lower()], aggfunc=np.sum).reset_index() #####comment
        
        # month_list = []
        # for col in final_df.columns:
        # 	try:
        # 		curr_date = col
        # 		months = ((curr_date.year - start_date.year) * 12 + curr_date.month - start_date.month)+1
        # 		month_col = 'GROSS_MARGIN_CYCLE_'+str(months).zfill(2)
        # 		final_df = final_df.rename({col:month_col},axis=1)
        # 		month_list.append(month_col)
        # 	except Exception as e:
        # 		print(e)
        
        # final_df[month_list] = final_df[month_list].fillna(0)
        
        
        # final_df['INSERT_DATE'] = datetime.now().date()
        # final_df['DATE_KEY'] = str_res_max
        # measure_sale['INSERT_DATE'] = datetime.now().date()
        # measure_sale['DATE_KEY'] = measure_sale['BEG_MO'].dt.date.apply(lambda x: str(x).replace('-',''))
        
        # # final_df['SKU_SEQ_ID'] = pd.merge(final_df, db_dim_sku1[intesection(match_col, df_sales_map_copy.columns)+[
        # #                                        'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map_copy.columns), how='left')['SKU_SEQ_ID']
        
        # # final_df = final_df.applymap(
        # #         lambda x: x.date() if isinstance(x, datetime) else x)
        # measure_sale = measure_sale.applymap(
        #         lambda x: x.date() if isinstance(x, datetime) else x)
        
        #final_df.to_excel("GROSS_MARGIN_CYCLE.xlsx")
        
        s1 = time.time()
        ##success, nchunks, nrows, _ = write_pandas(ctx, renaming(final_df[intesection(final_df,db_table.iloc[:,1])]), 'FACT_GROSS_MARGIN_CYCLE', parallel=99)
        #success, nchunks, nrows, _ = write_pandas(ctx, renaming(measure_sale[intesection(measure_sale,db_table.iloc[:,1])]), 'FACT_GROSS_MARGIN_CYCLE', parallel=99)
        
        s2 = time.time()
        total_insert_time = total_insert_time + (s2-s1)
        
        print('''###############################################
        
        '10. SALE CYCLE'
        
        ###############################################''')
        
        start = time.time()
        db_table = pd.read_excel('Test Python Schema Tables (2).xlsx',
                                     sheet_name='FACT_SALES_CYCLE')
        
        
        match_col = ['LOCATION', 'COUNTRY', 'DIVISION','INVENTORY_TYPE','TRIM_SKU', 'DEAL_ID'] #DEAL ID is added by manoj
        df_sales_map_copy = df_sales_map_copy.rename({'BOM':'BEG_MO'},axis=1)
        df_inv_map_copy = df_inv_map_copy.rename({'BOM':'BOM_INV_DATE'},axis=1)
        
        #create measure_sale groupby match_col and BEG_MO column
        measure_sale = df_sales_map_copy.fillna('NA').groupby(intesection(match_col, df_sales_map_copy.columns) + ['BEG_MO'])[intesection(['NET_SALES_$'], df_sales_map_copy.columns)].sum().reset_index()
        #get first_sale_date
        first_sale = measure_sale.fillna('NA').groupby(intesection(match_col, df_sales_map_copy.columns))['BEG_MO'].min().reset_index()
        first_sale = first_sale.rename({'BEG_MO':'FIRST_SALE_DATE'},axis=1)
        #get last_sales_date
        last_sale = measure_sale.fillna('NA').groupby(intesection(match_col, df_sales_map_copy.columns))['BEG_MO'].max().reset_index()
        last_sale = last_sale.rename({'BEG_MO':'LAST_SALES_DATE'},axis=1)
        #get first_receipt_date
        first_receipt = df_inv_map_copy.fillna('NA').groupby(intesection(match_col, df_inv_map_copy.columns))['BOM_INV_DATE'].min().reset_index()
        first_receipt = first_receipt.rename({'BOM_INV_DATE':'FIRST_RECEIPT_DATE'},axis=1)
        
        last_receipt = df_inv_map_copy.fillna('NA').groupby(intesection(match_col, df_inv_map_copy.columns))['BOM_INV_DATE'].max().reset_index()
        last_receipt = last_receipt.rename({'BOM_INV_DATE':'LAST_RECEIPT_DATE'},axis=1)
        
        #merge measure_sale and first_sale_date
        measure_sale = pd.merge(measure_sale, first_sale, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        #merge measure_sale and last_sales_date
        measure_sale = pd.merge(measure_sale, last_sale, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        #merge measure_sale and first_receipt_date
        measure_sale = pd.merge(measure_sale, first_receipt, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        
        measure_sale = pd.merge(measure_sale, last_receipt, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        
        #create introduction_date
        measure_sale['INTRODUCTION_DATE'] = measure_sale[['FIRST_SALE_DATE','FIRST_RECEIPT_DATE']].min(axis=1)
        
        measure_sale['NET_SALES_$'] = measure_sale['NET_SALES_$'].fillna(0)
        measure_sale[intesection(match_col, df_sales_map_copy.columns)] = measure_sale[intesection(match_col, df_sales_map_copy.columns)].fillna('NA')
        
        measure_sale['BEG_MO'] = measure_sale['BEG_MO'].dt.date
        
        measure_sale = temp_bucket(measure_sale,'sale',deal_id)
        measure_sale.columns
        ###############################################
        
        measure_sale=pd.merge(measure_sale,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']],on=intesection(match_col, df_sales_map_copy.columns),how='left').drop_duplicates()
        
        test24=measure_sale.loc[:,['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MO']]
        test24.columns=['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MON']
        for i in range(24):
            test24['SALES_CYCLE_'+str(i+1).zfill(2)] = test24.FIRST_SALE_DATE + pd.DateOffset(months = i)
        test24.set_index(['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MON'],inplace=True)
        test24=test24.stack()
        test24=test24.reset_index()
        test24.columns=['SKU_SEQ_ID','FIRST_SALE_DATE','BEG_MON','MONTH','BEG_MO']
        
        # test24=pd.merge(test24,fact_sale_db[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']],on=intesection(match_col, df_sales_map_copy.columns),how='left').drop_duplicates()
        fact_sale_db_copy=fact_sale_db.copy()
        fact_sale_db_copy['BEG_MO']=pd.to_datetime(fact_sale_db_copy['BEG_MO'])
        test24['BEG_MO']=pd.to_datetime(test24['BEG_MO'])
        test24.set_index(['SKU_SEQ_ID','BEG_MO'],inplace=True)
        fact_sale_db_copy.set_index(['SKU_SEQ_ID','BEG_MO'],inplace=True)
        test24['NET_SALES_$']=fact_sale_db_copy['NET_SALES_$']
        test24=test24.reset_index()
        final=pd.pivot_table(test24, values='NET_SALES_$', columns='MONTH', index=['SKU_SEQ_ID','BEG_MON'], aggfunc=np.sum).reset_index()
        final.rename(columns={'BEG_MON':'BEG_MO'},inplace=True)
        
        measure_sale['BEG_MO']=pd.to_datetime(measure_sale['BEG_MO'])
        final["BEG_MO"] = pd.to_datetime(final["BEG_MO"])
        
        measure_sale=pd.merge(measure_sale,final,on=['SKU_SEQ_ID','BEG_MO'],how='left')
        measure_sale['INSERT_DATE'] = datetime.now().date()
        
        measure_sale['DATE_KEY'] = measure_sale['BEG_MO'].dt.date.astype(str).str.replace('-','')
        measure_sale = measure_sale.applymap(
                lambda x: x.date() if isinstance(x, datetime) else x)
        
        ################################################
        # start_date = measure_sale['BEG_MO'].min()
        
        # str_res_max = measure_sale['BEG_MO'].apply(lambda x: int(str(x).replace('-','')))
        
        # res = max(measure_sale['BEG_MO'])
        
        # final_df = pd.pivot_table(measure_sale, values='NET_SALES_$', columns='BEG_MO', index=intesection(match_col, df_sales_map_copy.columns)+ ['FIRST_SALE_DATE','FIRST_RECEIPT_DATE','INTRODUCTION_DATE','LAST_SALES_DATE','LAST_RECEIPT_DATE']+[i for i in measure_sale if 'bucket' in i.lower()], aggfunc=np.sum).reset_index()
        # final_df.columns
        # month_list = []
        # for col in final_df.columns:
        # 	try:
        # 		curr_date = col
                
        # 		months = ((curr_date.year - start_date.year) * 12 + curr_date.month - start_date.month)+1
        # 		month_col = 'SALES_CYCLE_'+str(months).zfill(2)
        # 		final_df = final_df.rename({col:month_col},axis=1)
        # 		month_list.append(month_col)
        # 	except Exception as e:
        # 		print(e)
        
        # final_df[month_list] = final_df[month_list].fillna(0)
        
        # final_df['INSERT_DATE'] = datetime.now().date()
        # final_df['DATE_KEY'] = str_res_max
        
        # final_df['SKU_SEQ_ID'] = pd.merge(final_df, db_dim_sku1[intesection(match_col, df_sales_map_copy.columns)+[
        #                                        'SKU_SEQ_ID']], on=intesection(match_col, df_sales_map_copy.columns), how='left')['SKU_SEQ_ID']
        
        # final_df = final_df.applymap(
        #         lambda x: x.date() if isinstance(x, datetime) else x)
        
        # #final_df.to_excel("SALE CYCLE.xlsx")
        
        s1 = time.time()
        ## success, nchunks, nrows, _ = write_pandas(ctx, renaming(final_df[intesection(final_df,db_table.iloc[:,1])]), 'FACT_SALES_CYCLE', parallel=99)
        #success, nchunks, nrows, _ = write_pandas(ctx, renaming(measure_sale[intesection(measure_sale,db_table.iloc[:,1])]), 'FACT_SALES_CYCLE', parallel=99)
        
        s2 = time.time()
        total_insert_time = total_insert_time + (s2-s1)
        
        print('''###############################################
        
        '11. SELL THRU'
        
        ###############################################''')
        
        match_col = ['LOCATION', 'COUNTRY', 'DIVISION', 'INVENTORY_TYPE', 'TRIM_SKU']
        
        #create measure_sale groupby match_col and BEG_MO column
        measure_sale = df_sales_map_copy.groupby(intesection(match_col, df_sales_map_copy.columns) + ['BEG_MO'],dropna=False)[intesection(['COGS_$', 'NET_SALES_$', 'QUANTITY_SOLD', 'GROSS_MARGIN_$'], df_sales_map_copy.columns)].sum().reset_index()
        #get first_sale_date
        first_sale = measure_sale.groupby(intesection(match_col, df_sales_map_copy.columns),dropna=False)['BEG_MO'].min().reset_index()
        first_sale = first_sale.rename({'BEG_MO':'FIRST_SALE_DATE'},axis=1)
        #get last_sales_date
        last_sale = measure_sale.groupby(intesection(match_col, df_sales_map_copy.columns),dropna=False)['BEG_MO'].max().reset_index()
        last_sale = last_sale.rename({'BEG_MO':'LAST_SALES_DATE'},axis=1)
        #merge measure_sale and first_sale_date
        measure_sale = pd.merge(measure_sale, first_sale, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        #merge measure_sale and last_sales_date
        measure_sale = pd.merge(measure_sale, last_sale, how='left', on=intesection(match_col, df_sales_map_copy.columns))
        
        #create measure_invt groupby match_col and BEG_MO column
        measure_invt = df_inv_map_copy.groupby(intesection(match_col, df_inv_map_copy.columns) + ['BOM_INV_DATE'],dropna=False)[intesection(['QUANTITY_ON_HAND','TOTAL_COST_EXTENDED'], df_inv_map_copy.columns)].sum().reset_index()
        #get first_receipt_date
        first_receipt = measure_invt.groupby(intesection(match_col, df_inv_map_copy.columns),dropna=False)['BOM_INV_DATE'].min().reset_index()
        first_receipt = first_receipt.rename({'BOM_INV_DATE':'FIRST_RECEIPT_DATE'},axis=1)
        #merge measure_inv and first_receipt_date
        measure_invt = pd.merge(measure_invt, first_receipt, how='left', on=intesection(match_col, df_inv_map_copy.columns))
        
        #create aggregate dictionary
        agg_dict = {}
        for i in intesection(['QUANTITY_SOLD','NET_SALES_$','COGS_$','GROSS_MARGIN_$'], df_sales_map_copy.columns):
            agg_dict[i] = 'sum'
        agg_dict['FIRST_SALE_DATE'] = 'min'
        agg_dict['LAST_SALES_DATE'] = 'max'
        
        #for every BOM_INV_DATE - 
        #	filter BEG_MO from measure_sale and create df measure_sale_latest
        #	groupby above measure_sale_latest df for agg_dict
        #	merge measure_sale_latest with measure_inv and take all records from measure_sale_latest(left join)
        #	append to final_measure_invt
        final_measure_invt=pd.DataFrame()
        for res in sorted(set(measure_invt['BOM_INV_DATE'])):
        	measure_sale_latest = measure_sale[(measure_sale['BEG_MO']==res)]
        	measure_sale_latest = measure_sale_latest.groupby(intesection(match_col, df_sales_map_copy.columns),dropna=False).agg(agg_dict).reset_index()
        	measure_sale_latest['BOM_INV_DATE'] = res
        	temp_measure_invt = pd.merge(measure_sale_latest, measure_invt, on=intesection(match_col, df_sales_map_copy.columns)+['BOM_INV_DATE'], how='left')
        	final_measure_invt = final_measure_invt.append(temp_measure_invt)
        final_measure_invt = pd.merge(final_measure_invt, measure_invt, on=['BOM_INV_DATE']+intesection(match_col, df_sales_map_copy.columns), how='left')
        
        #remove duplicate merged columns
        l1 = []
        l2 = []
        for i in final_measure_invt.columns:
            if ('_x' in i):
                l1.append(i)
            elif ('_y' in i):
                l2.append(i)
        final_measure_invt.drop(l1, axis=1,inplace=True)
        final_measure_invt.columns = [i.replace('_y','') for i in final_measure_invt.columns]
        
        ###############################################################################################################
        #unit sold x+1
        
        #for every sku - take inv bom + 1 month as bom from mo_sales
        
        #means - for dec 2020 take:
        #		units sold x+1 is (x months quantity sold $-use ----- QUANTITY_SOLD) from jan 2021 from mo_sales
        #		sales $x+1 is (x months sales $-use ----- NET_SALES_$) from jan 2021 from mo_sales
        #		cogs x+1 is (x months cogs $-use ----- COGS_$) from jan 2021 from mo_sales
        
        ###############################################################################################################
        
        sell_through = final_measure_invt.copy()
        #sell_through.to_excel('sell_through.xlsx',index=False)
        sell_through['INTRODUCTION_DATE'] = sell_through[['FIRST_SALE_DATE','FIRST_RECEIPT_DATE']].min(axis=1)
        
        def sell_through_calculation(df,months):
        	'''
        	create df with bom_inv_date
        	create df with bom_inv_date + 1
        	merge both on trim sku
        
        	Parameters:
        		df: sell_through dataframe
        		months (int): 1 = BOM_INV_DATE+1, 2 = BOM_INV_DATE+2, 3 = BOM_INV_DATE+3
        	'''
        	
        	sell_through_temp1 = df.copy()
        	sell_through_temp2 = df.copy()
        
        	sell_through_temp1['SALES_IN_MONTH_DATE'] = sell_through_temp1['BOM_INV_DATE']
        	sell_through_temp2['SALES_IN_MONTH_DATE'] = sell_through_temp2['BOM_INV_DATE'] + pd.DateOffset(months=months)
        
        	sell_through_final = pd.merge(sell_through_temp1, sell_through_temp2, on=['SALES_IN_MONTH_DATE']+intesection(match_col, df_sales_map_copy.columns), how='right')
        
        	sell_through_final = sell_through_final.rename({'BOM_INV_DATE_y':'BOM_INV_DATE'},axis=1)
        	sell_through_final = sell_through_final.rename({'FIRST_RECEIPT_DATE_y':'FIRST_RECEIPT_DATE'},axis=1)
        	sell_through_final = sell_through_final.rename({'INTRODUCTION_DATE_y':'INTRODUCTION_DATE'},axis=1)
        	sell_through_final = sell_through_final.rename({'FIRST_SALE_DATE_y':'FIRST_SALE_DATE'},axis=1)
        	sell_through_final = sell_through_final.rename({'LAST_SALES_DATE_y':'LAST_SALES_DATE'},axis=1)
        	sell_through_final = sell_through_final.rename({'QUANTITY_ON_HAND_y':'QUANTITY_ON_HAND'},axis=1)
        	sell_through_final = sell_through_final.rename({'TOTAL_COST_EXTENDED_y':'TOTAL_COST_EXTENDED'},axis=1)
        	sell_through_final = sell_through_final.rename({'NET_SALES_$_x':'SALES_AMOUNT_X_'+str(months)},axis=1)
        	sell_through_final = sell_through_final.rename({'QUANTITY_SOLD_x':'UNITS_SOLD_X_'+str(months)},axis=1)
        	sell_through_final = sell_through_final.rename({'COGS_$_x':'COGS_AMOUNT_X_'+str(months)},axis=1)
        
        	sell_through_final.drop('SALES_IN_MONTH_DATE', axis=1, inplace=True)
        
        	l1 = []
        	l2 = []
        	for i in sell_through_final.columns:
        		if ('_x' in i):
        			l1.append(i)
        		elif ('_y' in i):
        			l2.append(i)
        	sell_through_final.drop(l1, axis=1,inplace=True)
        	sell_through_final.drop(l2, axis=1,inplace=True)
        
        	sell_through_final = pd.merge(sell_through_final, measure_invt, on=['BOM_INV_DATE']+intesection(match_col, df_sales_map_copy.columns), how='right')
        
        	l1 = []
        	l2 = []
        	for i in sell_through_final.columns:
        		if ('_x' in i):
        			l1.append(i)
        		elif ('_y' in i):
        			l2.append(i)
        	sell_through_final.drop(l1, axis=1,inplace=True)
        	sell_through_final.columns = [i.replace('_y','') for i in sell_through_final.columns]
        
        	return sell_through_final
        
        sell_through_final_1 = sell_through_calculation(sell_through,1)
        sell_through_final_2 = sell_through_calculation(sell_through,2)
        sell_through_final_3 = sell_through_calculation(sell_through,3)
        # a=pd.concat([sell_through_final_1, sell_through_final_2, sell_through_final_3], axis=1).T.drop_duplicates().T
        # b=a.loc[:,~a.columns.duplicated()]
        #create dataframe by concat sell_through_final_1,sell_through_final_2,sell_through_final_3 and remove duplicate columns by transposing
        # final_df = pd.concat([sell_through_final_1, sell_through_final_2, sell_through_final_3], axis=1).T.drop_duplicates().T ###11/08
        final_df = pd.concat([sell_through_final_1, sell_through_final_2, sell_through_final_3], axis=1)  ####11/08
        final_df=final_df.loc[:,~final_df.columns.duplicated()]  ###11/08
        
        fillna_cols = ['COGS_AMOUNT_X_1','COGS_AMOUNT_X_2','COGS_AMOUNT_X_3','UNITS_SOLD_X_1','UNITS_SOLD_X_2','UNITS_SOLD_X_3','SALES_AMOUNT_X_1','SALES_AMOUNT_X_2','SALES_AMOUNT_X_3']
        final_df[fillna_cols] = final_df[fillna_cols].fillna(0)
        
        #COGS 3 month total
        final_df['THREE_MONTH_TOTAL_COGS'] = final_df['COGS_AMOUNT_X_1']+final_df['COGS_AMOUNT_X_2']+final_df['COGS_AMOUNT_X_3']
        
        #Units 3 month total
        final_df['THREE_MONTH_TOTAL_UNITS'] = final_df['UNITS_SOLD_X_1']+final_df['UNITS_SOLD_X_2']+final_df['UNITS_SOLD_X_3']
        
        #Sales 3 month total
        final_df['THREE_MONTH_TOTAL_SALES'] = final_df['SALES_AMOUNT_X_1']+final_df['SALES_AMOUNT_X_2']+final_df['SALES_AMOUNT_X_3']
        
        #COGS per unit
        final_df['COGS_UNIT'] = final_df['THREE_MONTH_TOTAL_COGS']/final_df['THREE_MONTH_TOTAL_UNITS']
        
        #ADJUSTED QUANTITY_ON_HAND => if QUANTITY_ON_HAND = 0 then TOTAL_COST_EXTENDED/COGS_UNIT else QUANTITY_ON_HAND
        final_df.loc[(final_df['QUANTITY_ON_HAND']==0), 'ADJ_QTY_DEMAND'] = final_df['TOTAL_COST_EXTENDED'].div(final_df['COGS_UNIT'].where(final_df['COGS_UNIT'] != 0, np.nan))
        final_df.loc[(final_df['QUANTITY_ON_HAND']!=0), 'ADJ_QTY_DEMAND'] = final_df['QUANTITY_ON_HAND']
        
        #COST PER UNIT (CPU) => if ADJ_QTY_DEMAND != 0 then TOTAL_COST_EXTENDED/ADJ_QTY_DEMAND else 0
        #final_df.loc[(final_df['ADJ_QTY_DEMAND']!=0), 'CPU'] = final_df['TOTAL_COST_EXTENDED']/final_df['ADJ_QTY_DEMAND']
        final_df.loc[(final_df['ADJ_QTY_DEMAND']!=0), 'CPU'] = final_df['TOTAL_COST_EXTENDED'].div(final_df['ADJ_QTY_DEMAND'].where(final_df['ADJ_QTY_DEMAND'] != 0, np.nan))
        final_df.loc[(final_df['ADJ_QTY_DEMAND']==0), 'CPU'] = 0
        
        #COGS_CHECK_PERCENTAGE_DIFF => if (COGS_UNIT == 0 OR CPU == 0) then COGS_CHECK_PERCENTAGE_DIFF = NA else COGS_PERCENT_UNIT = (COGS_UNIT-CPU)/CPU
        final_df['COGS_CHECK_PERCENTAGE_DIFF'] = (final_df['COGS_UNIT']-final_df['CPU']).div(final_df['CPU'].where(final_df['CPU'] != 0, np.nan))
        
        #UNITS_SOLD_X_1_ADJ => if COGS_CHECK_PERCENTAGE_DIFF<-0.3 | COGS_CHECK_PERCENTAGE_DIFF>0.3 then COGS_AMOUNT_X_1/CPU else UNITS_SOLD_X_1
        final_df.loc[(final_df['COGS_CHECK_PERCENTAGE_DIFF']<-0.3) | (final_df['COGS_CHECK_PERCENTAGE_DIFF']>0.3), 'UNITS_SOLD_X_1_ADJ'] = final_df['COGS_AMOUNT_X_1'].div(final_df['CPU'].where(final_df['CPU'] != 0, np.nan))
        final_df.loc[(final_df['COGS_CHECK_PERCENTAGE_DIFF']>-0.3) | (final_df['COGS_CHECK_PERCENTAGE_DIFF']<0.3), 'UNITS_SOLD_X_1_ADJ'] = final_df['UNITS_SOLD_X_1']
        
        #UNITS_SOLD_X_2_ADJ => if COGS_CHECK_PERCENTAGE_DIFF<-0.3 | COGS_CHECK_PERCENTAGE_DIFF>0.3 then COGS_AMOUNT_X_2/CPU else UNITS_SOLD_X_2
        final_df.loc[(final_df['COGS_CHECK_PERCENTAGE_DIFF']<-0.3) | (final_df['COGS_CHECK_PERCENTAGE_DIFF']>0.3), 'UNITS_SOLD_X_2_ADJ'] = final_df['COGS_AMOUNT_X_2'].div(final_df['CPU'].where(final_df['CPU'] != 0, np.nan))
        final_df.loc[(final_df['COGS_CHECK_PERCENTAGE_DIFF']>-0.3) | (final_df['COGS_CHECK_PERCENTAGE_DIFF']<0.3), 'UNITS_SOLD_X_2_ADJ'] = final_df['UNITS_SOLD_X_2']
        
        #UNITS_SOLD_X_3_ADJ => if COGS_CHECK_PERCENTAGE_DIFF<-0.3 | COGS_CHECK_PERCENTAGE_DIFF>0.3 then COGS_AMOUNT_X_3/CPU else UNITS_SOLD_X_3
        final_df.loc[(final_df['COGS_CHECK_PERCENTAGE_DIFF']<-0.3) | (final_df['COGS_CHECK_PERCENTAGE_DIFF']>0.3), 'UNITS_SOLD_X_3_ADJ'] = final_df['COGS_AMOUNT_X_3'].div(final_df['CPU'].where(final_df['CPU'] != 0, np.nan))
        final_df.loc[(final_df['COGS_CHECK_PERCENTAGE_DIFF']>-0.3) | (final_df['COGS_CHECK_PERCENTAGE_DIFF']<0.3), 'UNITS_SOLD_X_3_ADJ'] = final_df['UNITS_SOLD_X_3']
        
        fillna_cols = ['UNITS_SOLD_X_1_ADJ','UNITS_SOLD_X_2_ADJ','UNITS_SOLD_X_3_ADJ']
        final_df[fillna_cols] = final_df[fillna_cols].fillna(0)
        
        #UNIT_X+1 => min(UNITS_SOLD_X_1_ADJ,ADJ_QTY_DEMAND)
        final_df['UNITS_ST_1'] = final_df[['UNITS_SOLD_X_1_ADJ', 'ADJ_QTY_DEMAND']].min(axis=1)
        final_df['COST_ST_1'] = final_df['UNITS_ST_1']*final_df['CPU']
        
        #UNIT_X+2 => min(UNITS_SOLD_X_2_ADJ,ADJ_QTY_DEMAND-UNITS_ST_1)
        final_df['temp1'] = final_df['ADJ_QTY_DEMAND']-final_df['UNITS_ST_1']
        final_df['UNITS_ST_2'] = final_df[['UNITS_SOLD_X_2_ADJ', 'temp1']].min(axis=1)
        final_df['COST_ST_2'] = final_df['UNITS_ST_2']*final_df['CPU']
        
        #UNIT_X+3 => min(UNITS_SOLD_X_3_ADJ,ADJ_QTY_DEMAND-UNITS_ST_1-UNITS_ST_2)
        final_df['temp1'] = final_df['ADJ_QTY_DEMAND']-final_df['UNITS_ST_1']-final_df['UNITS_ST_2']
        final_df['UNITS_ST_3'] = final_df[['UNITS_SOLD_X_3_ADJ', 'temp1']].min(axis=1)
        final_df['COST_ST_3'] = final_df['UNITS_ST_3']*final_df['CPU']
        
        final_df.drop('temp1', axis=1, inplace=True)
        
        final_df = final_df.rename({'QUANTITY_ON_HAND':'GROSS_INVENTORY_QUANTITY'},axis=1)
        final_df = final_df.rename({'TOTAL_COST_EXTENDED':'GROSS_INVENTORY_COST'},axis=1)
        
        #change date types from datetime to date
        final_df['FIRST_SALE_DATE'] = final_df['FIRST_SALE_DATE'].dt.date
        final_df['BOM_INV_DATE'] = final_df['BOM_INV_DATE'].dt.date
        final_df['INTRODUCTION_DATE'] = final_df['INTRODUCTION_DATE'].dt.date
        final_df['FIRST_RECEIPT_DATE'] = final_df['FIRST_RECEIPT_DATE'].dt.date
        final_df['LAST_SALES_DATE'] = final_df['LAST_SALES_DATE'].dt.date
        
        #add date_key column
        final_df['DATE_KEY'] = final_df['BOM_INV_DATE'].astype(str).str.replace('-','')
        
        #add deal_id column
        final_df['DEAL_ID'] = deal_id
        
        #add sku column
        final_df['SKU'] = final_df['TRIM_SKU']
        
        #add insert_date column
        final_df['INSERT_DATE'] = datetime.now().date()
        
        #add sku_sequence_id
        final_df['SKU_SEQ_ID'] = pd.merge(final_df, db_dim_sku1[intesection(match_col, df_sales_map_copy.columns)+['SKU_SEQ_ID']], on=intesection(match_col, df_sales_map_copy.columns), how='left')['SKU_SEQ_ID']
        final_df.isna().sum()
        
        final_df.sort_values(['SKU_SEQ_ID','DATE_KEY'],inplace=True)
        fact_inv_db_copy=fact_inv_db.copy()  ##3
            # db_demand.set_index('')
        fact_inv_db_copy.sort_values(['SKU_SEQ_ID','DATE_KEY'],inplace=True)
        final_df.reset_index(drop=True,inplace=True)
        fact_inv_db_copy.reset_index(drop=True,inplace=True)
            # db_demand.isna().sum() ##
        final_df[[i for i in fact_inv_db_copy.columns if 'BUCKET' in i]] = fact_inv_db_copy[[i for i in fact_inv_db_copy.columns if 'BUCKET' in i]]
            
        #final_df.to_excel('sell_through_output_TVS_JM.xlsx',index=False)
        # final_df = pd.merge(final_df, fact_inv_db[["INTRODUCTION_BUCKET", "LAST_RECEIPT_DATE_BUCKET", "FIRST_SALE_DATE_BUCKET", "LAST_SALE_DATE_BUCKET", "SKU_SEQ_ID"]], on = ["SKU_SEQ_ID"], how = "left").drop_duplicates() ##change made by manoj ###13/08
        
        # final_df = pd.merge(final_df, fact_inv_db[intesection(match_col, df_sales_map_copy.columns)+["INTRODUCTION_BUCKET", "LAST_RECEIPT_DATE_BUCKET", "FIRST_SALE_DATE_BUCKET", "LAST_SALE_DATE_BUCKET"]], on = intesection(match_col, df_sales_map_copy.columns), how = "left").drop_duplicates() ##change made by manoj
        #insert into db
        # fact_inv_db[[i for i in fact_inv_db.columns if 'BUCKET' in i ]]
        # len(set(final_df['TRIM_SKU'].unique())-set(fact_inv_db['TRIM_SKU'].unique()))
        # fact_sale_db[fact_sale_db['TRIM_SKU']=='RMW6532CM']
        db_table = pd.read_excel('Test Python Schema Tables (2).xlsx',sheet_name='FACT_SELL_THRU')
        db_table.iloc[:,1].to_list()
        # final_df['SKU_SEQ_ID'].nunique()
        # db_dim_cat.groupby(['COUNTRY', 'DIVISION', 'INVENTORY_TYPE', 'SKU'])[['DEAL_ID']].sum()
        final_df
        #success, nchunks, nrows, _ = write_pandas(ctx, renaming(final_df[intesection(final_df,db_table.iloc[:,1])]), 'FACT_SELL_THRU', parallel=99)
        final_df[[i for i in final_df.columns if 'DATE' in i]].isna().sum()
        end = time.time()
        
        print("TIME TO COMPLETE: "+str(end-start))
        
        print("""###############################################################################################""")
        
        
        print ("""
        #####################       
        INSERTION TIME
        ####################
        """, total_insert_time)
        
        
        print ("""
        #####################       
        TOTAL TIME for EXECUTION
        ####################
        """, time.time() - final_start)
        
        execution = time.time() - final_start
        
        print (f"----------Around {total_insert_time*100/execution} is used in insertion-------------")
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
