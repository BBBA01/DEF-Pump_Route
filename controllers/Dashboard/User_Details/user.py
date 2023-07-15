import pandas as pd

def UserDetails(user_id,cnxn):
    df=pd.read_sql_query(f'''
Select 
       UO.OfficeId,
	   O.MasterOfficeId,
	   O.OfficeName,
	   ANU.UserType,
	   ANU.FirstName,
	   ANU.SurName

from
UserOfficeMapper UO 

Left Outer Join
Office O ON O.OfficeId=UO.OfficeId

Left Outer Join
AspNetUsers ANU ON UO.UserId=ANU.Id

Where UO.UserId='{user_id}'
;
''',cnxn)
    return df.to_dict(orient='records')
