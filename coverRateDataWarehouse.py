import cx_Oracle as oracle
import pandas as pd
from datetime import timedelta, date

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
query = """
Select h.rzdate,h.branchno,h.customercode,b.plucode,b.counts,b.pftotal,b.ystotal
  From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select h.rzdate,h.branchno,h.customercode,b.plucode,-b.counts As counts, -b.pftotal as pftotal, -b.yttotal as ystotal
  From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s'
"""


for i in pd.date_range(date(2017,3,10),date(2017,10,31)):
    d = i.strftime('%Y-%m-%d')
    print(d)
    data = pd.read_sql(query % (d,d,d,d),con=con)
    data.to_csv('F:\\DataWarehouse\\salesDetail\\%s.csv' % d,index=False)
        
        


#df[['RZDATE','COUNTS','PFTOTAL']].groupby(['RZDATE']).value.sum().compute()