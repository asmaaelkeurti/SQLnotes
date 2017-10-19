# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 13:55:59 2017

@author: 150972
"""

import cx_Oracle as oracle
import pandas as pd
from datetime import timedelta, date

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
month = [
            ['2017-09-01','2017-09-30'],
            ['2017-08-01','2017-08-31'],
            ['2017-07-01','2017-07-31'],
            ['2017-06-01','2017-06-30'],
            ['2017-05-01','2017-05-31'],
            ['2017-04-01','2017-04-30'],
            ['2017-03-01','2017-03-31'],
            ['2017-02-01','2017-02-28'],
            ['2017-01-01','2017-01-31'],
            
            ['2016-12-01','2016-12-31'],
            ['2016-11-01','2016-11-30'],
            ['2016-10-01','2016-10-31'],
            ['2016-09-01','2016-09-30'],
            ['2016-08-01','2016-08-31'],
            ['2016-07-01','2016-07-31'],
            ['2016-06-01','2016-06-30'],
            ['2016-05-01','2016-05-31'],
            ['2016-04-01','2016-04-30'],
            ['2016-03-01','2016-03-31'],
            ['2016-02-01','2016-02-29'],
            ['2016-01-01','2016-01-31'],
        ]

day = [
       ['2017-10-10','2017-10-10'],
       ['2017-10-09','2017-10-09'],
       ['2017-10-09','2017-10-09']
       ]

query="""select branchno, customercode, plucode,round(sum(counts)*10000.0,0) counts, round(avg(jprice)*10000.0,0) jprice, round(avg(pfprice)*10000.0,0) pfprice, round(sum(ystotal)*10000.0,0) ystotal,round(sum(pftotal)*10000.0,0) pftotal from
(Select h.branchno,h.customercode,b.plucode,b.counts,b.jprice,b.pfprice,b.ystotal,b.pftotal
  From tmgpfsaledetail_rpt@hsrpt b,tmgpfsaleform_rpt@hsrpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select h.branchno,h.customercode,b.plucode,-b.counts As counts,b.jprice,b.pfprice, -b.yttotal as ystotal, -b.pftotal as pftotal
  From tmgpfreturndetail_rpt@hsrpt b,tmgpfreturnform_rpt@hsrpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s') group by branchno, customercode, plucode"""

query="""select s.branchno, s.customercode, s.plucode,
            round(sum(counts)*10000.0,0) counts, 
            round(avg(jprice)*10000.0,0) jprice, 
            round(avg(pfprice)*10000.0,0) pfprice, 
            round(sum(ystotal),0) ystotal,
            round(sum(pftotal),0) pftotal from
(Select h.branchno,h.customercode,b.plucode,b.counts,b.jprice,b.pfprice,b.ystotal,b.pftotal
  From tmgpfsaledetail_rpt@hsrpt b,tmgpfsaleform_rpt@hsrpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select h.branchno,h.customercode,b.plucode,-b.counts As counts,b.jprice,b.pfprice, -b.yttotal as ystotal, -b.pftotal as pftotal
  From tmgpfreturndetail_rpt@hsrpt b,tmgpfreturnform_rpt@hsrpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s') s,
     torgdrprelation r,tskuplu p,tskuetpparas e
where s.branchno = r.orgcode
      And s.plucode = p.plucode And r.preorgcode = e.orgcode And p.pluid = e.pluid
      And Exists(Select 1 From torgdrpdisp Where orgcode = s.branchno And remark Like '%%有效%%')  --有效配送中心
      And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
      And e.ismainetp = '1'
group by s.branchno, s.customercode, s.plucode"""

#for m in month:
#    print(m)
#    data = pd.read_sql(query % (m[0],m[1],m[0],m[1]),con=con)
#    data['COUNTS'] = data['COUNTS']/10000.0
#    data['JPRICE'] = data['JPRICE']/10000.0
#    data['PFPRICE'] = data['PFPRICE']/10000.0
#    data['YSTOTAL'] = data['YSTOTAL']/10000.0
#    data['PFTOTAL'] = data['PFTOTAL']/10000.0
#    data['TIMERANGE'] = m[0]
#    data.to_csv('c:\\Users\\150972\\Desktop\\data.csv',mode='a')
    
    
for i in pd.date_range(date(2016,1,1),date(2017,9,1)):
    d = i.strftime('%Y-%m-%d')
    print(d)
    data = pd.read_sql(query % (d,d,d,d),con=con)
    data['COUNTS'] = data['COUNTS']/10000.0
    data['JPRICE'] = data['JPRICE']/10000.0
    data['PFPRICE'] = data['PFPRICE']/10000.0
    data['YSTOTAL'] = data['YSTOTAL']/10000.0
    data['PFTOTAL'] = data['PFTOTAL']/10000.0
    data['TIMERANGE'] = d
    if i.strftime('%Y-%m-%d')==date(2016,1,1).strftime('%Y-%m-%d'):
        data.to_csv('c:\\Users\\150972\\Desktop\\data.csv',mode='a',index=False)
    else:
        data.to_csv('c:\\Users\\150972\\Desktop\\data.csv',mode='a',header=False, index=False)
    
 
 
 

 