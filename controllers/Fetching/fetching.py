import pyodbc 
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import warnings
warnings.filterwarnings('ignore')
from config.config import ConnectionString

def Fetching(StartingPointId):
    try:
        cnxn = pyodbc.connect(ConnectionString)

        CurrentStockDetails = pd.read_sql_query('select OfficeId,ProductTypeId,CurrentStock from CurrentStockDetails',cnxn)
        Office = pd.read_sql_query('select OfficeId,OfficeName,Longitude,Latitude from Office', cnxn)
        GodownMaster = pd.read_sql_query('Select OfficeId,Capacity from GodownMaster', cnxn)
        Sales = pd.read_sql_query('Select InvoiceDate,OfficeId,Quantity,Total from Sales where Total>0;', cnxn)

        GodownMaster=GodownMaster.groupby('OfficeId').agg('sum')
        GodownMaster.rename(columns={'Capacity':'totalCapacity'},inplace=True)
        Sales=pd.DataFrame(Sales.groupby('OfficeId')['Quantity'].mean())
        Sales.rename(columns={'Quantity':'avgSales'},inplace=True)


        df = pd.merge(Office, CurrentStockDetails, on="OfficeId", how="outer")
        df = pd.merge(df, GodownMaster, on="OfficeId", how="outer")
        df = pd.merge(df, Sales, on="OfficeId", how="outer")
        df['avgSales'].fillna(0,inplace=True)
        df.rename(columns={'OfficeId':'officeId','OfficeName':'officeName','Longitude':'longitude','Latitude':'latitude','CurrentStock':'currentStock','ProductTypeId':'productTypeId'},inplace=True)

        StartPoint=pd.read_sql_query(f'Select CityId,CityName,Latitude,Longitude from CityMaster where CityId={StartingPointId}', cnxn)
  
        return df,StartPoint
    except Exception as e:
        print(e)
        print("Error in fetching data")
        

