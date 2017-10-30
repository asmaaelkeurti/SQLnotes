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

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')

month = [
            #['2017-10-01','2017-10-31'],
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


query="""
Select c.orgcode,
       c.custcode,
       c.custname,
       c.kid,
       count(distinct s.Rzdate) RzCountsTotal,
       round(Sum(s.counts*e.lsavghjprice)*1000.0,0) As CostOfGoodsSold,
       round(sum(s.counts),0) as pscounts
  From (
Select h.rzdate,h.branchno,h.customercode,b.plucode,b.counts
  From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select h.rzdate,h.branchno,h.customercode,b.plucode,-b.counts As counts
  From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s'
       ) s,tdrpetpcustdetail c,torgdrprelation r,tskuplu p,tskuetpparas e
 Where s.branchno = c.orgcode 
   And s.customercode = c.custcode 
   And s.branchno = r.orgcode
   and c.isperfect='1' 
   And s.plucode = p.plucode And r.preorgcode = e.orgcode And p.pluid = e.pluid
   And Exists(Select 1 From torgdrpdisp Where orgcode = s.branchno And remark Like '%%有效%%')  --有效配送中心
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
   And e.ismainetp = '1'
 Group By c.orgcode,c.custcode, c.kid,c.custname
"""

for m in month:
    print(m)
    data = pd.read_sql(query % (m[0],m[1],m[0],m[1]),con=con)
    data['COSTOFGOODSSOLD'] = data['COSTOFGOODSSOLD']/1000.0
    data['date'] = m[0]
    if m[0] == month[0][0]:
        data.to_csv('F:\\DataWarehouse\\10-20-sum.csv',index=False)
    else:
        data.to_csv('F:\\DataWarehouse\\10-20-sum.csv',mode='a',header=False, index=False)
