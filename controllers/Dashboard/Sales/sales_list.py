import pandas as pd
from functools import cache 
import time
import pyodbc
import sys
sys.path.append('C:/Users/skyha/Desktop/DEF-Pump_Route')
from config.config import ConnectionString
from collections import defaultdict



def godown_list(office_id,from_date,to_date,level,cnxn):
    # start_time = time.time()  # Record the start time
    df1=pd.read_sql_query(f'''
  WITH cte_org AS (
    SELECT
        ofs.OfficeId,
        ofs.MasterOfficeId,
        ofs.OfficeTypeId,
        ofs.OfficeName,
        mo.OfficeName AS MasterOfficeName,
        0 AS Level
    
    FROM Office ofs
    LEFT JOIN Office mo ON mo.OfficeId = ofs.MasterOfficeId
    WHERE ofs.OfficeId = '{office_id}'

    UNION ALL

    SELECT
        e.OfficeId,
        e.MasterOfficeId,
        e.OfficeTypeId,
        e.OfficeName,
        o.OfficeName AS MasterOfficeName,
        Level + 1 AS Level
        
    FROM Office e
    INNER JOIN cte_org o ON o.OfficeId = e.MasterOfficeId
)

SELECT
    ct.MasterOfficeId As masterOfficeId,
    ct.MasterOfficeName As masterOfficeName,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
    ot.color As officeTypeColor,
    ct.level,
	S.InvoiceDate As incomeDate,
	S.totalIncome,
	S.Quantity,
	S.FuelRateId,
	FR.ProductTypeId As productId,
	Pt.ProductTypeName As productName,
	UM.UnitName As unitName,
	UM.UnitShortName As unitShortName,
	Um.SingularShortName As singularShortName,
    S.Rate As rate,
    PT.Color As color
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select SUM(Total) As totalIncome,Sum(Quantity) As Quantity,InvoiceDate,FuelRateId,OfficeId,Rate
From Sales
Where
IsDeleted=0 AND
InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}'
Group By
InvoiceDate,FuelRateId,OfficeId,Rate
)S On ct.officeId=S.OfficeId 

Left join
FuelRate FR ON FR.FuelRateId=S.FuelRateId
Left join
ProductType PT ON FR.ProductTypeId=PT.ProductTypeId
Left join
UnitMaster UM ON PT.PrimaryUnitId=UM.UnitId
WHERE
    ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    df2=pd.read_sql_query(f'''
   WITH cte_org AS (
    SELECT
        ofs.OfficeId,
        ofs.MasterOfficeId,
        ofs.OfficeTypeId,
        ofs.OfficeName,
        mo.OfficeName AS MasterOfficeName,
        0 AS Level
    FROM Office ofs
    LEFT JOIN Office mo ON mo.OfficeId = ofs.MasterOfficeId
    WHERE ofs.OfficeId = '{office_id}'

    UNION ALL

    SELECT
        e.OfficeId,
        e.MasterOfficeId,
        e.OfficeTypeId,
        e.OfficeName,
        o.OfficeName AS MasterOfficeName,
        Level + 1 AS Level      
    FROM Office e
    INNER JOIN cte_org o ON o.OfficeId = e.MasterOfficeId
)

SELECT
    ct.MasterOfficeId As masterOfficeId,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
	E.VoucherDate As expenseDate,
	E.totalExpense
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select officeId,SUM(Amount) As totalExpense,VoucherDate
From Expense
Where
IsDeleted=0 AND
VoucherDate>='{from_date}' AND VoucherDate<='{to_date}'
Group By
officeId, VoucherDate
)E On ct.officeId=E.OfficeId

WHERE
    ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    # end_time = time.time()  # Record the end time
    # execution_time = end_time - start_time  # Calculate the execution time in seconds
    # print(f"godown_list execution time: {execution_time:.4f} seconds")  # Print the execution time with 4 decimal places

    return df1,df2


def total_sales_based_on_office_body(df):
    # start_time = time.time()
    alldata=[]
    for office in (df[df["level"]==1]["officeId"].unique()):
            if(len(df[df["masterOfficeId"].str.lower()==office.lower()])):
                totalIncome=df[df["masterOfficeId"].str.lower()==office.lower()]["totalIncome"].sum()
                for innerOffice in df[df["masterOfficeId"].str.lower()==office.lower()]["officeId"].unique():
                    totalIncome+=df[df["masterOfficeId"].str.lower()==innerOffice.lower()]["totalIncome"].sum()
                alldata.append({
                    "officeId":office,
                    "officeName":df[df["masterOfficeId"].str.lower()==office.lower()]["masterOfficeName"].unique()[0],
                    "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                    "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                    "totalIncome":totalIncome
                })
            else:
                totalIncome=df[df["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
                alldata.append({
                    "officeId":office,
                    "officeName":df[df["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
                    "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                    "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                    "totalIncome":totalIncome
                })
    final_df=pd.DataFrame(alldata)
    try:
        final_df=(final_df.sort_values(by=["officeType","officeName"],ascending=[False,False],key=lambda x:x.str.lower())).reset_index(drop=True)
    except:
        print("No Data Found")
    # end_time = time.time()
    # execution_time = end_time - start_time
    # print(f"total_sales_based_on_office_body execution time: {execution_time:.4f} seconds")

    return final_df.to_dict('records')

def sales_based_on_admin_body(date_range, df1, df2):
    # start_time = time.time()

    df1["incomeDate"] = pd.to_datetime(df1["incomeDate"])
    df2["expenseDate"] = pd.to_datetime(df2["expenseDate"])

    df1_grouped = df1.groupby(["incomeDate", "productId", "productName", "unitName", "unitShortName", "singularShortName", "rate", "color"]).agg({"totalIncome": "sum", "Quantity": "sum"}).reset_index()
    df1_grouped.rename({"totalIncome": "totalSales", "Quantity": "qty"}, axis=1, inplace=True)

    sales_grouped = df1.groupby(["incomeDate", "officeId", "officeName", "officeType"]).agg({"totalIncome": "sum"}).reset_index()
    sales_grouped.rename({"totalIncome": "totalSales"}, axis=1, inplace=True)

    expense_grouped = df2.groupby(["expenseDate", "officeId", "officeName", "officeType"]).agg({"totalExpense": "sum"}).reset_index()

    merged_list = pd.merge(sales_grouped, expense_grouped, on=["officeId", "officeName", "officeType"], how="outer").fillna(0)

    alldata = []
    productdata = []

    for i in date_range:
        requested_date = pd.to_datetime(i).strftime("%Y-%m-%d")

        filtered_sales = df1_grouped[df1_grouped["incomeDate"] == i]
        filtered_products = df1_grouped[df1_grouped["incomeDate"] == i]

        income = filtered_sales["totalSales"].sum()
        expense = expense_grouped[expense_grouped["expenseDate"] == i]["totalExpense"].sum()

        lst_office = merged_list[merged_list["incomeDate"] == i].to_dict(orient="records")
        lst_product = filtered_products.to_dict(orient="records")

        alldata.append({
            "requestedDate": requested_date,
            "totalIncome": income,
            "totalExpense": expense,
            "lstOffice": lst_office
        })

        productdata.append({
            "requestedDate": requested_date,
            "lstproduct": lst_product
        })

    # end_time = time.time()
    # execution_time = end_time - start_time
    # print(f"sales_based_on_admin_body execution time: {execution_time:.4f} seconds")

    return alldata, productdata

# df1, df2= godown_list('46A1C7B7-6885-4F74-7ADE-08DAFE23C727', '2023-01-01', '2023-07-10', -1, cnxn = pyodbc.connect(ConnectionString))
# print(sales_based_on_admin_body(date_range=pd.date_range('2023-01-01', '2023-07-01'), df1=df1, df2=df2))

def sales_based_on_admin(office_id, is_admin, from_date, to_date, cnxn):
    # start_time = time.time()
    
    # Fetch the initial data
    df1, df2 = godown_list(office_id, from_date, to_date, -1, cnxn)
    
    date_range = pd.date_range(from_date, to_date)
    sales_based_on_date = []
    sales_based_on_product = []
    sales_based_on_office = []

    if is_admin in [0, 1, 2, 3, 4, 5, 6]:
        if is_admin in [4, 5, 6]:
            sales_agg = df1.groupby(["incomeDate"]).agg({"totalIncome": "sum"}).reset_index()
            expenses_agg = df2.groupby(["expenseDate"]).agg({"totalExpense": "sum"}).reset_index()

        if is_admin in [0, 1, 2, 3]:
            df1_office_specific = df1[df1["officeId"].str.lower() == office_id.lower()]
            df2_office_specific = df2[df2["officeId"].str.lower() == office_id.lower()]
            sales_agg = df1_office_specific.groupby(["incomeDate"]).agg({"totalIncome": "sum"}).reset_index()
            expenses_agg = df2_office_specific.groupby(["expenseDate"]).agg({"totalExpense": "sum"}).reset_index()

        # Pre-aggregate product data
        product_agg = df1.groupby(["productId", "productName", "unitName", "unitShortName", "singularShortName", "rate", "color", "incomeDate"]).agg({"totalIncome": "sum", "Quantity": "sum"}).reset_index()
        
        for i in date_range:
            requested_date = pd.to_datetime(i).strftime("%Y-%m-%d")
            
            income = sales_agg[sales_agg["incomeDate"] == i]["totalIncome"].sum()
            expense = expenses_agg[expenses_agg["expenseDate"] == i]["totalExpense"].sum()

            product_list = product_agg[product_agg["incomeDate"] == i][["productId", "productName", "unitName", "unitShortName", "singularShortName", "totalIncome", "Quantity", "rate", "color"]]
            product_list.rename({"totalIncome": "totalSales", "Quantity": "qty"}, axis=1, inplace=True)
            product_list = product_list.astype({"totalSales": int, "qty": int, "productId": int})

            sales_based_on_date.append({
                "requestedDate": requested_date,
                "totalIncome": income,
                "totalExpense": expense,
                "lstOffice": []
            })
            sales_based_on_product.append({
                "requestedDate": requested_date,
                "lstproduct": product_list.to_dict(orient="records")
            })

        if is_admin in [0, 1, 2, 3]:
            total_sales_office = df1_office_specific[df1_office_specific["level"] == 0].groupby("officeId").agg({"totalIncome": "sum"}).reset_index()
            total_sales_office.rename(columns={"totalIncome": "totalIncomeOffice"}, inplace=True)
            total_sales_office = pd.merge(total_sales_office, df1_office_specific[df1_office_specific["level"] == 0][["officeId", "officeName", "officeTypeColor", "officeType"]], on="officeId", how="left")

            sales_based_on_office = total_sales_office.to_dict(orient="records")
        
    # end_time = time.time()
    # execution_time = end_time - start_time
    # print(f"sales_based_on_admin execution time: {execution_time:.4f} seconds") 
    return sales_based_on_date, sales_based_on_product, sales_based_on_office
# print(sales_based_on_admin('06D5DDA0-6834-4EA1-183D-08DAF95AD4EF', 6 , '2023-01-01', '2023-07-01', cnxn = pyodbc.connect(ConnectionString)))








