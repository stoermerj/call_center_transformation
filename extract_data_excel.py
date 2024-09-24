import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
import re


DATA_PATH = 'data'
TODAY = datetime.today().strftime('%Y-%m-%d')
df = pd.DataFrame(columns=['business_date', 'call_type', 'call_direction', 'segment', 'product', 'call_count',
                           'call_duration'])

#function returns all file names in folder
def folder_crawler(): 
    for root, dir_names, file_names in os.walk(DATA_PATH):
        return file_names

#function returns all excel sheets in excel file
def find_excel_sheet_names(excel_file):
    excel_file_sheet_names = pd.ExcelFile(DATA_PATH+'/'+str(excel_file))  
    #return timedates, add sunday
    return excel_file_sheet_names.sheet_names

#function returns a dataframe with an excel sheet
def read_the_excel(excel, tab_name):
    df = pd.read_excel(DATA_PATH+'/'+str(excel), sheet_name=tab_name)
    return df

#retrieve the data from the cells and move it to dataframe
def retrieve_and_send_data(dataframe, tab_name):
    
    business_date = tab_name
    call_type_service = dataframe.iat[27,0]
    call_type_sales = dataframe.iat[39,0]

    #service relates calls
    inbound_clc_service_new_data = {'business_date': business_date, 'call_type': call_type_service, 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': dataframe.iat[29, 1], 'call_duration': dataframe.iat[29, 2]}
    df.loc[len(df)] = inbound_clc_service_new_data

    inbound_mun_service_new_data = {'business_date': business_date, 'call_type': call_type_service, 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': dataframe.iat[30, 1], 'call_duration': dataframe.iat[30, 2]}
    df.loc[len(df)] = inbound_mun_service_new_data

    inbound_psc_service_new_data = {'business_date': business_date, 'call_type': call_type_service, 
                'call_direction': 'inbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': dataframe.iat[31, 1], 'call_duration': dataframe.iat[31, 2]}
    df.loc[len(df)] = inbound_psc_service_new_data

    outbound_clc_service_new_data = {'business_date': business_date, 'call_type': call_type_service, 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': dataframe.iat[32, 1], 'call_duration': dataframe.iat[32, 2]}
    df.loc[len(df)] = outbound_clc_service_new_data

    outbound_psc_service_new_data = {'business_date': business_date, 'call_type': call_type_service, 
                'call_direction': 'outbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': dataframe.iat[33, 1], 'call_duration': dataframe.iat[33, 2]}
    df.loc[len(df)] = outbound_psc_service_new_data

    outbound_mun_service_new_data = {'business_date': business_date, 'call_type': call_type_service, 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': dataframe.iat[30, 1], 'call_duration': dataframe.iat[30, 2]}
    df.loc[len(df)] = outbound_mun_service_new_data

    #sales related calls
    inbound_clc_sales_new_data = {'business_date': business_date, 'call_type': call_type_sales, 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': dataframe.iat[41, 1], 'call_duration': dataframe.iat[41, 2]}
    df.loc[len(df)] = inbound_clc_sales_new_data

    inbound_mun_sales_new_data = {'business_date': business_date, 'call_type': call_type_sales, 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': dataframe.iat[42, 1], 'call_duration': dataframe.iat[42, 2]}
    df.loc[len(df)] = inbound_mun_sales_new_data

    inbound_psc_sales_new_data = {'business_date': business_date, 'call_type': call_type_sales, 
                'call_direction': 'inbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': dataframe.iat[43, 1], 'call_duration': dataframe.iat[43, 2]}
    df.loc[len(df)] = inbound_psc_sales_new_data

    outbound_clc_sales_new_data = {'business_date': business_date, 'call_type': call_type_sales, 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': dataframe.iat[44, 1], 'call_duration': dataframe.iat[44, 2]}
    df.loc[len(df)] = outbound_clc_sales_new_data

    outbound_psc_sales_new_data = {'business_date': business_date, 'call_type': call_type_sales, 
                'call_direction': 'outbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': dataframe.iat[45, 1], 'call_duration': dataframe.iat[45, 2]}
    df.loc[len(df)] = outbound_psc_sales_new_data

    outbound_mun_sales_new_data = {'business_date': business_date, 'call_type': call_type_sales, 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': dataframe.iat[46, 1], 'call_duration': dataframe.iat[46, 2]}
    df.loc[len(df)] = outbound_mun_sales_new_data


def sunday_addition(sunday):
    business_date = sunday

    #service relates calls
    inbound_clc_service_new_data = {'business_date': business_date, 'call_type': 'Service', 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = inbound_clc_service_new_data

    inbound_mun_service_new_data = {'business_date': business_date, 'call_type': 'Service', 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = inbound_mun_service_new_data

    inbound_psc_service_new_data = {'business_date': business_date, 'call_type': 'Service', 
                'call_direction': 'inbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = inbound_psc_service_new_data

    outbound_clc_service_new_data = {'business_date': business_date, 'call_type': 'Service', 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = outbound_clc_service_new_data

    outbound_psc_service_new_data = {'business_date': business_date, 'call_type': 'Service', 
                'call_direction': 'outbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = outbound_psc_service_new_data

    outbound_mun_service_new_data = {'business_date': business_date, 'call_type': 'Service', 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = outbound_mun_service_new_data

    #sales related calls
    inbound_clc_sales_new_data = {'business_date': business_date, 'call_type': 'Sales', 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = inbound_clc_sales_new_data

    inbound_mun_sales_new_data = {'business_date': business_date, 'call_type': 'Sales', 
                'call_direction': 'inbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = inbound_mun_sales_new_data

    inbound_psc_sales_new_data = {'business_date': business_date, 'call_type': 'Sales', 
                'call_direction': 'inbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = inbound_psc_sales_new_data

    outbound_clc_sales_new_data = {'business_date': business_date, 'call_type': 'Sales', 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'CLC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = outbound_clc_sales_new_data

    outbound_psc_sales_new_data = {'business_date': business_date, 'call_type': 'Sales', 
                'call_direction': 'outbound', 'segment': 'PSC', 'product': 'PSC', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = outbound_psc_sales_new_data

    outbound_mun_sales_new_data = {'business_date': business_date, 'call_type': 'Sales', 
                'call_direction': 'outbound', 'segment': 'APC', 'product': 'HGH', 
                'call_count': 0, 'call_duration': 0}
    df.loc[len(df)] = outbound_mun_sales_new_data

"""def folder_retrieval():
    file_names = folder_crawler()
    for excel in file_names:
        excel_sheets = find_excel_sheet_names(excel)
        for tab_name in excel_sheets:
            if re.search('Gesamt', tab_name) == None:
                df = read_the_excel(excel,tab_name)
                retrieve_and_send_data(df, tab_name)
                datetime_tab_name= datetime.strptime(tab_name, '%d.%m.%Y').date()
                day_of_week= datetime.strptime(tab_name, '%d.%m.%Y').date().isoweekday()
                if day_of_week == 6:
                    datetime_sunday = datetime_tab_name + timedelta(days=1)
                    datetime_sunday_string = datetime_sunday.strftime('%d.%m.%Y')
                    sunday_addition(datetime_sunday_string)
            
folder_retrieval()
print(df)"""

