import pandas as pd

def dropdown_list(user_id,cnxn):
    OfficeId = pd.read_sql_query(
        f"""
    Select OfficeId from UserOfficeMapper Where UserId='{user_id}';
    """
    ,cnxn)
    if (len(OfficeId)!=0):
        OfficeId=OfficeId["OfficeId"].values[0]

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
        WHERE ofs.OfficeId = '{OfficeId}'

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
        ct.*,
        ot.OfficeTypeName
    FROM cte_org ct
    LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId
    WHERE
        (1 < 0 OR ct.Level <= 1)
    ORDER BY
        ct.Level;
    ''',cnxn)
        if (len(df)>1):
            df=df.iloc[1:]

        return df[["OfficeId","OfficeTypeName","OfficeName"]]
    
    else:
        return pd.DataFrame()