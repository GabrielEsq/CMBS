from numpy.core.fromnumeric import transpose
from numpy.lib.function_base import copy
import pandas as pd
import eikon as ek 
ek.set_app_key('d241df4abf1248f78d100e148241dfc267401d0f')
from datetime import datetime
import csv
import time
from openpyxl import load_workbook

todays_date_1pm = time.strftime("%m-%d-%Y 13:00:00")
todays_date_3pm = time.strftime("%m-%d-%Y 15:00:00")
todays_date_4pm = time.strftime("%m-%d-%Y 16:00:00")


#1PM SNAP
start_date_1PM= datetime.now().replace(hour=0, minute=0)
end_date_1PM= datetime.now().replace(hour=13, minute=0)

#3PM SNAP
start_date_3PM= datetime.now().replace(hour=14, minute=59)
end_date_3PM= datetime.now().replace(hour=15, minute=0)

#4PM SNAP
start_date_4PM= datetime.now().replace(hour=15, minute=59)
end_date_4PM= datetime.now().replace(hour=23, minute=59)


class Treasury_loader:

    instruments = []
    def __init__(self):
        path_tbills = r'C:\Users\U6075486\Desktop\CMBS Treasuries\T-Bill.csv'
        tbills_df = pd.read_csv(path_tbills)
        self.instruments = tbills_df["cusips"].tolist()
        
    
    def calculate_treasuries(self,start_date,end_date):
        data_grid = ek.get_timeseries(self.instruments, start_date=start_date,end_date=end_date, interval = 'hour', fields= "CLOSE")
        return data_grid.fillna(method='ffill')

loader=Treasury_loader()
dfall = loader.calculate_treasuries(start_date_1PM,end_date_4PM)
Snap_1pm = (dfall[dfall.index==todays_date_1pm]) # 1pm
Snap_3pm = (dfall[dfall.index==todays_date_3pm]) # 3pm
Snap_4pm = (dfall[dfall.index==todays_date_4pm]) # 4pm


class swap_loader:

    instruments = []
    def __init__(self):
        path_swap_curve = r'C:\Users\U6075486\Desktop\CMBS Treasuries\Swap Curve.csv'
        swapcurve_df = pd.read_csv(path_swap_curve)
        self.instruments = swapcurve_df["cusip"].tolist()
        
    
    def calculate_swap(self,start_date,end_date):
        data_grid = ek.get_timeseries(self.instruments, start_date=start_date,end_date=end_date, interval = 'hour', fields= "CLOSE")
        return data_grid.fillna(method='ffill')

loader=swap_loader()
dfall_Swap = loader.calculate_swap(start_date_1PM,end_date_4PM)
Snap_swap_1pm = (dfall_Swap[dfall_Swap.index==todays_date_1pm]) # 1pm
Snap_swap_3pm = (dfall_Swap[dfall_Swap.index==todays_date_3pm]) # 3pm
Snap_swap_4pm = (dfall_Swap[dfall_Swap.index==todays_date_4pm]) # 4pm


class Swap_Spread_Calculator: 

    def calculate(self, snap_swap, snap_treasury):
       
        Treasury_Calculation = self.__calculate_treasury(snap_treasury)  
        return  {
            "Swap_Sprd_2YR" : (snap_swap['USDSB3L2Y='].iloc[0] - Treasury_Calculation['TSY_2YR'])*100, 
            "Swap_Sprd_3YR" : (snap_swap['USDSB3L3Y='].iloc[0] - Treasury_Calculation['TSY_3YR'])*100,
            "Swap_Sprd_4YR" : (snap_swap['USDSB3L4Y='].iloc[0] - Treasury_Calculation['TSY_4YR'])*100,
            "Swap_Sprd_5YR" : (snap_swap['USDSB3L5Y='].iloc[0] - Treasury_Calculation['TSY_5YR'])*100,
            "Swap_Sprd_6YR" : (snap_swap['USDSB3L6Y='].iloc[0] - Treasury_Calculation['TSY_6YR'])*100,
            "Swap_Sprd_7YR" : (snap_swap['USDSB3L7Y='].iloc[0] - Treasury_Calculation['TSY_7YR'])*100,
            "Swap_Sprd_8YR" : (snap_swap['USDSB3L8Y='].iloc[0] - Treasury_Calculation['TSY_8YR'])*100,
            "Swap_Sprd_9YR" : (snap_swap['USDSB3L9Y='].iloc[0] - Treasury_Calculation['TSY_9YR'])*100,
            "Swap_Sprd_10YR" : (snap_swap['USDSB3L10Y='].iloc[0] - Treasury_Calculation['TSY_10YR'])*100,
            "Swap_Sprd_11YR" : (snap_swap['USDSB3L11Y='].iloc[0] - Treasury_Calculation['TSY_11YR'])*100,
            "Swap_Sprd_12YR" : (snap_swap['USDSB3L12Y='].iloc[0] - Treasury_Calculation['TSY_12YR'])*100,
            "Swap_Sprd_13YR" : (snap_swap['USDSB3L13Y='].iloc[0] - Treasury_Calculation['TSY_13YR'])*100,
            "Swap_Sprd_14YR" : (snap_swap['USDSB3L14Y='].iloc[0] - Treasury_Calculation['TSY_14YR'])*100,
            "Swap_Sprd_15YR" : (snap_swap['USDSB3L15Y='].iloc[0] - Treasury_Calculation['TSY_15YR'])*100,
            "Swap_Sprd_20YR" : (snap_swap['USDSB3L20Y='].iloc[0] - Treasury_Calculation['TSY_20YR'])*100,
            "Swap_Sprd_30YR" : (snap_swap['USDSB3L30Y='].iloc[0] - Treasury_Calculation['TSY_30YR'])*100,
        }
    
    def __calculate_treasury(self, dataframe):
       return {
            "TSY_2YR" : dataframe['US2YT=RR'].iloc[0], 
            "TSY_3YR" : dataframe['US3YT=RR'].iloc[0],
            "TSY_4YR" : dataframe['US3YT=RR'].iloc[0] + (dataframe['US5YT=RR'].iloc[0]-dataframe['US3YT=RR'].iloc[0])/2,
            "TSY_5YR" : dataframe['US5YT=RR'].iloc[0], 
            "TSY_6YR" : dataframe['US5YT=RR'].iloc[0] + (dataframe['US7YT=RR'].iloc[0]-dataframe['US5YT=RR'].iloc[0])/2,  
            "TSY_7YR" : dataframe['US7YT=RR'].iloc[0], 
            "TSY_8YR" : dataframe['US7YT=RR'].iloc[0] + (dataframe['US10YT=RR'].iloc[0]-dataframe['US7YT=RR'].iloc[0])/3,
            "TSY_9YR" : dataframe['US7YT=RR'].iloc[0] + (dataframe['US10YT=RR'].iloc[0]-dataframe['US7YT=RR'].iloc[0])*2/3,
            "TSY_10YR" : dataframe['US10YT=RR'].iloc[0],
            "TSY_11YR" : dataframe['US10YT=RR'].iloc[0] + (dataframe['US30YT=RR'].iloc[0]-dataframe['US10YT=RR'].iloc[0])/20,
            "TSY_12YR" : dataframe['US10YT=RR'].iloc[0] + (dataframe['US30YT=RR'].iloc[0]-dataframe['US10YT=RR'].iloc[0])*2/20,
            "TSY_13YR" : dataframe['US10YT=RR'].iloc[0] + (dataframe['US30YT=RR'].iloc[0]-dataframe['US10YT=RR'].iloc[0])*3/20,
            "TSY_14YR" : dataframe['US10YT=RR'].iloc[0] + (dataframe['US30YT=RR'].iloc[0]-dataframe['US10YT=RR'].iloc[0])*4/20,
            "TSY_15YR" : dataframe['US10YT=RR'].iloc[0] + (dataframe['US30YT=RR'].iloc[0]-dataframe['US10YT=RR'].iloc[0])*5/20,
            "TSY_20YR" : dataframe['US10YT=RR'].iloc[0] + (dataframe['US30YT=RR'].iloc[0]-dataframe['US10YT=RR'].iloc[0])/2, 
            "TSY_30YR" : dataframe['US30YT=RR'].iloc[0]
        }




Swap_Test = Swap_Spread_Calculator()
Swap_Test.calculate(Snap_swap_1pm, Snap_1pm)
Swap_Test.calculate(Snap_swap_3pm, Snap_3pm)
Swap_Test.calculate(Snap_swap_4pm, Snap_4pm)

#Tresury

Snap_1pm_dic = Snap_1pm.to_dict("records")
Snap_3pm_dic = Snap_3pm.to_dict("records")
Snap_4pm_dic = Snap_4pm.to_dict("records")


#Swap 

Snap_swap_1pm_dic = Snap_swap_1pm.to_dict("records")
Snap_swap_3pm_dic = Snap_swap_3pm.to_dict("records")
Snap_swap_4pm_dic = Snap_swap_4pm.to_dict("records")

#Historical data

historical_curves_data = {"date": datetime.now().date()}

snap_swap_1pm_mod = {"[1PM]" + k: v for k, v in Snap_swap_1pm_dic[0].items()} 
snap_swap_3pm_mod = {"[3PM]" + k: v for k, v in Snap_swap_3pm_dic[0].items()}
snap_swap_4pm_mod = {"[4PM]" + k: v for k, v in Snap_swap_4pm_dic[0].items()}


snap_1pm_mod = {"[1PM]" + k: v for k, v in Snap_1pm_dic[0].items()} 
snap_3pm_mod = {"[3PM]" + k: v for k, v in Snap_3pm_dic[0].items()}
snap_4pm_mod = {"[4PM]" + k: v for k, v in Snap_4pm_dic[0].items()}


historical_curves_data.update(Swap_Test.calculate(Snap_swap_1pm, Snap_1pm))
historical_curves_data.update({'space1': ' '})
historical_curves_data.update(snap_swap_1pm_mod)
historical_curves_data.update({'space2': ' '})
historical_curves_data.update(snap_swap_3pm_mod)
historical_curves_data.update({'space3': ' '})
historical_curves_data.update(snap_swap_4pm_mod)
historical_curves_data.update({'space4': ' '})
historical_curves_data.update(snap_1pm_mod)
historical_curves_data.update({'space5': ' '})
historical_curves_data.update(snap_3pm_mod)
historical_curves_data.update({'space6': ' '})
historical_curves_data.update(snap_4pm_mod)

data_excel = "Historical_curves_CMBS.xlsx"
df = pd.DataFrame(list(historical_curves_data.items()))
df_1 = df.set_index(0).T

print(df_1.columns)

#writer = pd.ExcelWriter(data_excel, engine="xlsxwriter")
#df_1.to_excel(writer, sheet_name="Historical Curves", index= False, header= False)
#writer.save()
#writer.close()
#writer=None

reader = pd.read_excel(data_excel)
writer = pd.ExcelWriter(data_excel, engine='openpyxl',mode="a")
writer.sheets = dict((ws.title, ws) for ws in writer.book.worksheets)
reader = pd.read_excel(data_excel)
df_1.to_excel(writer, sheet_name="Historical Curves", index=False,header=False,startrow=len(reader)+1)
writer.close()