import pandas as pd

def Godown_list_level(office_id,level,cnxn):
    df=pd.read_sql_query(f'''
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
	ct.OfficeId,
     ct.masterOfficeId,
		ct.OfficeName,
        ot.OfficeTypeName As officeType,
		ct.Latitude,
		ct.Longitude,
        ot.OfficeTypeName,
		CS.ProductTypeId,
		CS.CurrentStock As totalStock,
		GM.GodownId,
		GM.GodownTypeId,
		GM.GodownName,
		GM.Capacity,
		GM.Status,
		GM.IsReserver,
		GT.GodownTypeName,
		GP.CurrentStock,
		s.avgSales,
        PT.ProductTypeName,
        PT.color
    FROM cte_org ct
    LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId
	Left Join
	GodownMaster GM ON GM.OfficeId=ct.OfficeId
	Left Join
GodownTypeMaster GT ON GM.GodownTypeId=GT.GodownTypeId
Left Join
GodownProductMapper GP ON GP.GodownId=GM.GodownId
LEFT JOIN
    (
        SELECT
            OfficeId,
            AVG(Quantity) AS avgSales
        FROM
            Sales
        WHERE
            Total > 0
        GROUP BY
            OfficeId
    ) s ON ct.OfficeId = s.OfficeId
	
	LEFT JOIN
	CurrentStockDetails CS ON CS.OfficeId=ct.OfficeId And CS.ProductTypeId=GP.ProductId
    LEFT JOIN
    ProductType PT ON PT.ProductTypeId=CS.ProductTypeId
    WHERE
        ({level} < 0 OR ct.Level <= {level})
    ORDER BY
        ct.Level;
''',cnxn)
  
    return df
    
def Godown_list(office_id,is_admin,cnxn):
    result=[]
    if(is_admin==6 or is_admin==5):
        df=Godown_list_level(office_id,-1,cnxn)
        # df=df[df["GodownTypeId"]==2]
        grouped = df.groupby('ProductTypeId')
        
        for product_type, group in grouped:
            
            grouped_by_office = group.groupby('OfficeId')
            for office, group_office in grouped_by_office:

                data={
                    "productTypeId": int(product_type),
                    "productTypeName": group_office["ProductTypeName"].values[0],
                    "color": group_office["color"].values[0],
                    "officeId": group_office["OfficeId"].values[0],
                    "officeType":group_office["officeType"].values[0],
                    "officeName": group_office["OfficeName"].values[0],
                    "latitude": str(group_office['Latitude'].values[0]),
                    "longitude": str(group_office['Longitude'].values[0]),
                    "avgSales": group_office['avgSales'].values[0],
                    "totalStock": group_office['totalStock'].values[0],
                    "totalCapacity": sum(group_office['Capacity']),
                    "godownProducts": []
                }
                data["godownProducts"].append(group_office[["GodownTypeId","GodownName","Capacity","CurrentStock","Status","IsReserver"]].to_dict(orient='records'))
                result.append(data)
    elif is_admin==4:
        df=Godown_list_level(office_id,-1,cnxn)
        # df=df[df["GodownTypeId"]==2]
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        grouped = df.groupby('ProductTypeId')

        for product_type, group in grouped:
            
            grouped_by_office = group.groupby('OfficeId')
            for office, group_office in grouped_by_office:

                data={
                    "productTypeId": int(product_type),
                    "productTypeName": group_office["ProductTypeName"].values[0],
                    "color": group_office["color"].values[0],
                    "officeId": group_office["OfficeId"].values[0],
                    "officeType":group_office["officeType"].values[0],
                    "officeName": group_office["OfficeName"].values[0],
                    "latitude": str(group_office['Latitude'].values[0]),
                    "longitude": str(group_office['Longitude'].values[0]),
                    "avgSales": group_office['avgSales'].values[0],
                    "totalStock": group_office['totalStock'].values[0],
                    "totalCapacity": sum(group_office['Capacity']),
                    "godownProducts": []
                }
                data["godownProducts"].append(group_office[["GodownTypeId","GodownName","Capacity","CurrentStock","Status","IsReserver"]].to_dict(orient='records'))
                result.append(data)
    elif is_admin==1:
        df=Godown_list_level(office_id,1,cnxn)
        # df=df[df["GodownTypeId"]==2]
        grouped = df.groupby('ProductTypeId')

        for product_type, group in grouped:
            
            grouped_by_office = group.groupby('OfficeId')
            for office, group_office in grouped_by_office:

                data={
                    "productTypeId": int(product_type),
                    "productTypeName": group_office["ProductTypeName"].values[0],
                    "color": group_office["color"].values[0],
                    "officeId": group_office["OfficeId"].values[0],
                    "officeType":group_office["officeType"].values[0],
                    "officeName": group_office["OfficeName"].values[0],
                    "latitude": str(group_office['Latitude'].values[0]),
                    "longitude": str(group_office['Longitude'].values[0]),
                    "avgSales": group_office['avgSales'].values[0],
                    "totalStock": group_office['totalStock'].values[0],
                    "totalCapacity": sum(group_office['Capacity']),
                    "godownProducts": []
                }
                data["godownProducts"].append(group_office[["GodownTypeId","GodownName","Capacity","CurrentStock","Status","IsReserver"]].to_dict(orient='records'))
                result.append(data)
    
    elif is_admin==3:
        df=Godown_list_level(office_id,1,cnxn)
        # df=df[df["GodownTypeId"]==2]
        df=df[df["officeType"]=="Wholesale Pumps"]
        grouped = df.groupby('ProductTypeId')

        for product_type, group in grouped:
            
            grouped_by_office = group.groupby('OfficeId')
            for office, group_office in grouped_by_office:

                data={
                    "productTypeId": int(product_type),
                    "productTypeName": group_office["ProductTypeName"].values[0],
                    "color": group_office["color"].values[0],
                    "officeId": group_office["OfficeId"].values[0],
                    "officeType":group_office["officeType"].values[0],
                    "officeName": group_office["OfficeName"].values[0],
                    "latitude": str(group_office['Latitude'].values[0]),
                    "longitude": str(group_office['Longitude'].values[0]),
                    "avgSales": group_office['avgSales'].values[0],
                    "totalStock": group_office['totalStock'].values[0],
                    "totalCapacity": sum(group_office['Capacity']),
                    "godownProducts": []
                }
                data["godownProducts"].append(group_office[["GodownTypeId","GodownName","Capacity","CurrentStock","Status","IsReserver"]].to_dict(orient='records'))
                result.append(data)
    elif is_admin==2:
        df=Godown_list_level(office_id,1,cnxn)
        # df=df[df["GodownTypeId"]==2]
   
        df=df[df["officeType"]=="Retail Pumps"]
        grouped = df.groupby('ProductTypeId')

        for product_type, group in grouped:
            
            grouped_by_office = group.groupby('OfficeId')
            for office, group_office in grouped_by_office:

                data={
                    "productTypeId": int(product_type),
                    "productTypeName": group_office["ProductTypeName"].values[0],
                    "color": group_office["color"].values[0],
                    "officeId": group_office["OfficeId"].values[0],
                    "officeType":group_office["officeType"].values[0],
                    "officeName": group_office["OfficeName"].values[0],
                    "latitude": str(group_office['Latitude'].values[0]),
                    "longitude": str(group_office['Longitude'].values[0]),
                    "avgSales": group_office['avgSales'].values[0],
                    "totalStock": group_office['totalStock'].values[0],
                    "totalCapacity": sum(group_office['Capacity']),
                    "godownProducts": []
                }
                data["godownProducts"].append(group_office[["GodownTypeId","GodownName","Capacity","CurrentStock","Status","IsReserver"]].to_dict(orient='records'))
                result.append(data)
    elif is_admin==0:
        df=Godown_list_level(office_id,1,cnxn)
        # df=df[df["GodownTypeId"]==2]
        df=df[df["officeId"].str.lower()==office_id.lower()]
      
        grouped = df.groupby('ProductTypeId')

        for product_type, group in grouped:
            
            grouped_by_office = group.groupby('OfficeId')
            for office, group_office in grouped_by_office:

                data={
                    "productTypeId": int(product_type),
                    "productTypeName": group_office["ProductTypeName"].values[0],
                    "color": group_office["color"].values[0],
                    "officeId": group_office["OfficeId"].values[0],
                    "officeType":group_office["officeType"].values[0],
                    "officeName": group_office["OfficeName"].values[0],
                    "latitude": str(group_office['Latitude'].values[0]),
                    "longitude": str(group_office['Longitude'].values[0]),
                    "avgSales": group_office['avgSales'].values[0],
                    "totalStock": group_office['totalStock'].values[0],
                    "totalCapacity": sum(group_office['Capacity']),
                    "godownProducts": []
                }
                data["godownProducts"].append(group_office[["GodownTypeId","GodownName","Capacity","CurrentStock","Status","IsReserver"]].to_dict(orient='records'))
                result.append(data)
    
    return result

    