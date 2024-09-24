import extract_data_excel
import re
import openpyxl
from datetime import datetime
from datetime import timedelta

#change crawler from local folder to sharepoint folder
#add drop off point to external application
#add libraries to be security conform

def main():
    def folder_retrieval():
        file_names = extract_data_excel.folder_crawler() 
        for excel in file_names: # loop through excel files in folder
            excel_sheets = extract_data_excel.find_excel_sheet_names(excel) 
            for tab_name in excel_sheets: #loop through excel sheets in excel file
                if re.search('Gesamt', tab_name) == None: #remove non crawlable excel sheets
                    df = extract_data_excel.read_the_excel(excel,tab_name) #extract dataframe from excel
                    extract_data_excel.retrieve_and_send_data(df, tab_name) #read through dataframe and extract to new dataframe
                    datetime_tab_name= datetime.strptime(tab_name, '%d.%m.%Y').date() #find date and transform of excel tab
                    day_of_week= datetime.strptime(tab_name, '%d.%m.%Y').date().isoweekday() #find the day of the week
                    if day_of_week == 6: #if date is saturday
                        datetime_sunday = datetime_tab_name + timedelta(days=1) #turn saturday to sunday
                        datetime_sunday_string = datetime_sunday.strftime('%d.%m.%Y') #turn datetime to string
                        extract_data_excel.sunday_addition(datetime_sunday_string) #add sunday data
    folder_retrieval()

    extract_data_excel.df['call_count'] = extract_data_excel.df['call_count'].fillna(0) #add 0 to call_count column
    extract_data_excel.df['call_duration'] = extract_data_excel.df['call_duration'].fillna(0) #add 0 to call_count column

    extract_data_excel.df.to_excel("aggregated_call_center_data_"+extract_data_excel.TODAY+".xlsx") #extract to excel
    print(extract_data_excel.df)

if __name__ == '__main__':
    main()