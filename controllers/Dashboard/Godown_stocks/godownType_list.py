import pandas as pd
from flask import jsonify

def GodownType_level(office_id,level,cnxn):
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
    ct.masterOfficeId,
	ct.OfficeId,
	ot.OfficeTypeName As officeType,
		ct.OfficeName,
		ct.Latitude,
		ct.Longitude,
		GM.GodownTypeId,
		GM.IsReserver,
        GT.GodownTypeName
        
    FROM cte_org ct
    LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

	Left Join
	GodownMaster GM ON GM.OfficeId=ct.OfficeId

    Left Join
    GodownTypeMaster GT ON GT.GodownTypeId=GM.GodownTypeId
	
    WHERE
        ({level} < 0 OR ct.Level <= {level})
    ORDER BY
        ct.Level;
''',cnxn)
    
    return df

def GodownType_Body(df,cnxn):
    df[["GodownTypeName","IsReserver"]].dropna(inplace=True)
    GodownTypeCount=df["GodownTypeName"].value_counts().to_dict()
    GodownTypeCount=pd.DataFrame.from_dict(GodownTypeCount,orient='index',columns=["Count"])
    GodownTypeCount.reset_index(inplace=True,drop=False)
    GodownTypeCount.rename(columns={'index':'GodownType'},inplace=True)

    IsReserverCount=df["IsReserver"].value_counts().to_dict()
    IsReserverCount=pd.DataFrame.from_dict(IsReserverCount,orient='index',columns=["Count"])
    IsReserverCount.reset_index(inplace=True,drop=False)
    IsReserverCount.rename(columns={'index':'Reserver'},inplace=True)
    IsReserverCount=IsReserverCount[IsReserverCount["Reserver"]==1][["Reserver","Count"]].to_dict(orient='records')
    GodownTypeCount=GodownTypeCount.to_dict(orient='records')
    GodownTypeCount.append({"GodownType":"Reserver","Count":IsReserverCount[0]["Count"] if len(IsReserverCount)>0 else 0})

    df2=pd.read_sql_query(f'''
Select * From GodownTypeMaster;
''',cnxn)
    new_list=df2.to_dict(orient='records')

    # Add "Count" key with a value of 0 to each dictionary
    # for item in new_list:
    #     item["Count"] = 0

    existing_methods = GodownTypeCount
    merged_list=existing_methods.copy()

    # Check if each item in new_list already exists in existing_list
    for item_new in new_list:
        exists = False
        for item_existing in existing_methods:
            if item_new["GodownTypeName"] == item_existing["GodownType"]:
                exists = True
                break
        if not exists:
            merged_list.append({"GodownType":item_new["GodownTypeName"],"Count":0})

    return jsonify(merged_list)

def GodownType(office_id,is_admin,cnxn):
    if(is_admin==6 or is_admin==5):
        df=GodownType_level(office_id,-1,cnxn)
        return(GodownType_Body(df,cnxn))
    elif(is_admin==4):
        df=GodownType_level(office_id,-1,cnxn)
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        return(GodownType_Body(df,cnxn))
    elif(is_admin==1):
        df=GodownType_level(office_id,1,cnxn)
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        return(GodownType_Body(df,cnxn))
    elif(is_admin==3):
        df=GodownType_level(office_id,1,cnxn)
        df=df[df["officeType"]=="Wholesale Pumps"]
        return(GodownType_Body(df,cnxn))
    elif(is_admin==2):
        df=GodownType_level(office_id,1,cnxn)
        df=df[df["officeType"]=="Retail Pumps"]
        return(GodownType_Body(df,cnxn))
    elif(is_admin==0):
        df=GodownType_level(office_id,0,cnxn)
        return(GodownType_Body(df,cnxn))
