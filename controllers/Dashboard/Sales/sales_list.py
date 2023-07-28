import pandas as pd

def godown_list(office_id,from_date,to_date,level,cnxn):
    df1=pd.read_sql_query(f'''
  WITH cte_org AS (
    SELECT
        ofs.OfficeId,
        ofs.MasterOfficeId,
        ofs.OfficeTypeId,
        ofs.OfficeName,
        mo.OfficeName AS MasterOfficeName,
        0 AS Level,
        ofs.OfficeAddress,
        ofs.RegisteredAddress,
        ofs.OfficeContactNo,
        ofs.OfficeEmail,
        ofs.GSTNumber,
        ofs.IsActive,
        ofs.Latitude,
        ofs.Longitude,
        ofs.GstTypeId
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
        Level + 1 AS Level,
        e.OfficeAddress,
        e.RegisteredAddress,
        e.OfficeContactNo,
        e.OfficeEmail,
        e.GSTNumber,
        e.IsActive,
        e.Latitude,
        e.Longitude,
        e.GstTypeId
    FROM Office e
    INNER JOIN cte_org o ON o.OfficeId = e.MasterOfficeId
)

SELECT
    ct.MasterOfficeId As masterOfficeId,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
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
        0 AS Level,
        ofs.OfficeAddress,
        ofs.RegisteredAddress,
        ofs.OfficeContactNo,
        ofs.OfficeEmail,
        ofs.GSTNumber,
        ofs.IsActive,
        ofs.Latitude,
        ofs.Longitude,
        ofs.GstTypeId
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
        Level + 1 AS Level,
        e.OfficeAddress,
        e.RegisteredAddress,
        e.OfficeContactNo,
        e.OfficeEmail,
        e.GSTNumber,
        e.IsActive,
        e.Latitude,
        e.Longitude,
        e.GstTypeId
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
VoucherDate>='{from_date}' AND VoucherDate<='{to_date}'
Group By
officeId, VoucherDate
)E On ct.officeId=E.OfficeId

WHERE
    ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    return df1,df2

def sales_based_on_admin(office_id,is_admin,from_date,to_date,cnxn):
    
    
    alldata=[]
    productdata=[]

    if is_admin==6:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,-1,cnxn)
        for i in date_range:
            income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
            expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
           
            Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
            Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalIncome":"sum"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
            Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalExpense":"sum"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
            Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
            Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
            
            
            alldata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": income,
                "totalExpense": expense,
                "lstOffice":Merged_list.to_dict(orient="records")
            })
            productdata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "lstproduct": Product_list.to_dict(orient="records")
                
            })


    elif is_admin==4:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,-1,cnxn)
        df1=df1[~((df1["officeType"]!="Company")& (df1["masterOfficeId"].str.lower()==office_id.lower()))]
        df2=df2[~((df2["officeType"]!="Company")& (df2["masterOfficeId"].str.lower()==office_id.lower()))]
    
        for i in date_range:
            income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
            expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
           
            Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
            Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalIncome":"sum"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
            Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalExpense":"sum"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
            Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
            Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
            
            
            alldata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": income,
                "totalExpense": expense,
                "lstOffice":Merged_list.to_dict(orient="records")
            })
            productdata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "lstproduct": Product_list.to_dict(orient="records")
                
            })
    elif is_admin==5:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,-1,cnxn)
        for i in date_range:
            income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
            expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
           
            Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
            Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalIncome":"sum"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
            Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalExpense":"sum"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
            Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
            Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
            
            
            alldata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": income,
                "totalExpense": expense,
                "lstOffice":Merged_list.to_dict(orient="records")
            })
            productdata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "lstproduct": Product_list.to_dict(orient="records")
                
            })
    elif is_admin==1:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,1,cnxn)
        if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        
            for i in date_range:
                income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
                expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
            
                Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
                Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalIncome":"sum"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
                Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalExpense":"sum"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
                Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
                Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
                Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
                
                
                alldata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "totalIncome": income,
                    "totalExpense": expense,
                    "lstOffice":Merged_list.to_dict(orient="records")
                })
                productdata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "lstproduct": Product_list.to_dict(orient="records")
                    
                })
    elif is_admin==3:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,1,cnxn)
        if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
            df1=df1[df1["officeType"]=="Wholesale Pumps"]
            df2=df2[df2["officeType"]=="Wholesale Pumps"]
            for i in date_range:
                income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
                expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
            
                Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
                Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalIncome":"sum"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
                Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalExpense":"sum"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
                Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
                Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
                Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
                
                
                alldata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "totalIncome": income,
                    "totalExpense": expense,
                    "lstOffice":Merged_list.to_dict(orient="records")
                })
                productdata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "lstproduct": Product_list.to_dict(orient="records")
                    
                })
    elif is_admin==2:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,1,cnxn)
        if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
            df1=df1[df1["officeType"]=="Retail Pumps"]
            df2=df2[df2["officeType"]=="Retail Pumps"]
            for i in date_range:
                income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
                expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
            
                Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
                Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalIncome":"sum"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
                Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId","officeName","officeType"]).agg({"totalExpense":"sum"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
                Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
                Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
                Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
                
                
                alldata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "totalIncome": income,
                    "totalExpense": expense,
                    "lstOffice":Merged_list.to_dict(orient="records")
                })
                productdata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "lstproduct": Product_list.to_dict(orient="records")
                    
                })

    elif is_admin==0:
        date_range=pd.date_range(from_date,to_date)
        df1,df2=godown_list(office_id,from_date,to_date,1,cnxn)
        df1=df1[df1["officeId"].str.lower()==office_id.lower()]
        df2=df2[df2["officeId"].str.lower()==office_id.lower()]
        for i in date_range:
            income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
            expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
           
            Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
            
            Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
            
            
            alldata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": income,
                "totalExpense": expense,
                "lstOffice":[]
            })
            productdata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "lstproduct": Product_list.to_dict(orient="records")
                
            })
    


    return alldata,productdata
