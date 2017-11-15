# -*- coding: utf-8 -*-
"""
Created on Sat Oct 21 09:52:59 2017

@author: 150972
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 15:40:10 2017

@author: 150972
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 13:39:52 2017

@author: 150972
"""
import cx_Oracle as oracle
import pandas as pd
from datetime import timedelta, date

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')

query="""
Select c.orgcode,
       (select orgname from torgdrprelation where orgcode = c.orgcode) orgname,
       (select preorgcode from torgdrprelation where orgcode = c.orgcode) fgscode,
       (select orgname from torgdrprelation where orgcode = (select preorgcode from torgdrprelation where orgcode = c.orgcode)) fgsname,
       c.custcode,
       c.custname,
       c.kid,
       f.postcode As etplevel,
       p.pluname,
       p.plucode,
       p.materialcode,
       count(distinct s.PFSaleNo) PFSaleCounts,
       count(distinct s.RZdate) RzCounts,
       round((case count(s.Rzdate) when 0 then 0 else round(sum(s.Counts)/count(distinct s.Rzdate),2) end),0) ACC,
       round(sum(s.counts),0) totalcounts,
       round(sum(s.ystotal)*1000,0) totalys,
       round(sum(s.pftotal)*1000,0) pftotal,
       c.isbangong,
       c.isjingpin,
       c.isxuesheng,
       c.isperfect
  From (
Select h.rzdate,h.branchno,h.customercode,b.plucode,b.counts,h.pfsaleno,b.pftotal,b.ystotal
  From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select NULL as rzdate,h.branchno,h.customercode,b.plucode,-b.counts As counts, NULL as pfsaleno, -b.pftotal as pftotal, -b.yttotal as ystotal
  From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s'
       ) s,tdrpetpcustdetail c,torgdrprelation r,tskuplu p,tskuetpparas e,TETPENTERPRISE f
 Where s.branchno = c.orgcode 
   And s.customercode = c.custcode 
   And s.branchno = r.orgcode
   and c.isperfect='1' 
   And s.plucode = p.plucode And r.preorgcode = e.orgcode And p.pluid = e.pluid
   and p.materialcode = 'ARP509041138E1H'
   and s.branchno=f.etpcode
   And Exists(Select 1 From torgdrpdisp Where orgcode = s.branchno And remark Like '%%有效%%')  --有效配送中心
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
   And e.ismainetp = '1'
 Group By c.orgcode, c.custcode, c.kid, f.postcode, p.pluname,p.plucode,p.materialcode,
c.isbangong, c.isjingpin, c.isxuesheng, c.custname,c.isperfect
"""

for i in pd.date_range(date(2017,6,1),date(2017,11,2)):
    d = i.strftime('%Y-%m-%d')
    print(d)
    data = pd.read_sql(query % (d,d,d,d),con=con)
    data['TOTALYS'] = data['TOTALYS']/1000.0
    data['PFTOTAL'] = data['PFTOTAL']/1000.0
    data['date'] = d
    if i.strftime('%Y-%m-%d')==date(2017,6,1).strftime('%Y-%m-%d'):
        data.to_csv('c:\\Users\\150972\\Desktop\\ARP509041138E1H.csv',header=True,index=False)
    else:
        data.to_csv('c:\\Users\\150972\\Desktop\\ARP509041138E1H.csv',mode='a',header=False, index=False)

