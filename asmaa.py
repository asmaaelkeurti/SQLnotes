from multiprocessing import Pool, TimeoutError
import pandas as pd
import time
import os
import cx_Oracle as oracle
from random import randint
from datetime import timedelta, date


con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
month = [   
            ['2017-11-01','2017-11-30','201711'],
            ['2017-10-01','2017-10-31','201710'],
            ['2017-09-01','2017-09-30','201709'],
            ['2017-08-01','2017-08-31','201708'],
            ['2017-07-01','2017-07-31','201707'],
            ['2017-06-01','2017-06-30','201706'],
            ['2017-05-01','2017-05-31','201705'],
            ['2017-04-01','2017-04-30','201704'],
            ['2017-03-01','2017-03-31','201703'],
            ['2017-02-01','2017-02-28','201702'],
            ['2017-01-01','2017-01-31','201701'],
            
            ['2016-12-01','2016-12-31','201612'],
            ['2016-11-01','2016-11-30','201611'],
            ['2016-10-01','2016-10-31','201610'],
            ['2016-09-01','2016-09-30','201609'],
            ['2016-08-01','2016-08-31','201608'],
            ['2016-07-01','2016-07-31','201607'],
            ['2016-06-01','2016-06-30','201606'],
            ['2016-05-01','2016-05-31','201605'],
            ['2016-04-01','2016-04-30','201604'],
            ['2016-03-01','2016-03-31','201603'],
            ['2016-02-01','2016-02-29','201602'],
            ['2016-01-01','2016-01-31','201601']
        ]

factquery = """
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

branchQuery = """
select H.OrgCode,H.EtpCode,sum(S.XsCount),sum(S.HJCost),sum(S.HxTotal)
from tSalPluDetail%s S,tstkkcjzhead%s K,tWslXsHead H, tcatcategory c, tskuplu p
where S.KcBillno=K.billno and K.YWBILLNO=h.billno and S.pluid=P.pluid and P.clsid = C.clsid
and to_char(h.jzdate,'yyyy-mm-dd') between  '%s' and '%s' 
and c.clscode like '0%%'
group by H.OrgCode,H.EtpCode"""

branchYSQuery = """
select fgscode, etpcode, round(sum(ystotal)*1000,0) as ystotal from
( select           h.OrgCode fgscode, 
                  h.etpcode etpcode,
                 (Case h.billtype When '0' Then b.ystotal Else -b.ystotal End) As ystotal
from twslxshead h, twslxsBody b,tskuplu c,tcatcategory d, officemember@finance o, notSigned2018@finance n
where     h.billno = b.billno
      and h.orgcode = o.orgcode and h.etpcode = o.custcode
      and c.clsid =  d.clsid
      and c.plucode = b.plucode
      and d.clscode like '0%%'
      and d.clscode not like '011%%'
      and c.materialcode = n.materialcode
      and n.lane2017 = '办公'
      and h.jzdate is not null
      and to_char(h.jzdate,'yyyy-mm-dd') between '%s' and '%s')
group by fgscode, etpcode
"""

endQuery = """
Select s.branchno,
       s.customercode,
       d.remark,
       round(sum(pftotal)*1000,0) pftotal
From (
       Select h.branchno,h.customercode,b.plucode,b.pftotal
       From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
       Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
         And h.rzdate Between '%s' And '%s'
Union All
      Select h.branchno,h.customercode,b.plucode,-b.pftotal As pftotal
      From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
      Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
      And h.rzdate Between  '%s' And '%s'
       ) s,torgdrprelation r,tskuplu p, torgdrpdisp d,officemember@finance o
Where  s.branchno = r.orgcode
   And s.plucode = p.plucode
   and d.orgcode = s.branchno
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
   and s.branchno = o.orgcode and s.customercode = o.custcode
Group By s.branchno,s.customercode,d.remark
"""

def f(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(query % (dd,dd,dd,dd),con=con)
    data.to_csv('F:\\DataWarehouse\\salesDetail\\%s.csv' % dd,index=False)
    return dd

def asmaa(m):
    data = pd.read_sql(endQuery % (m[0],m[1],m[0],m[1]), con=con)
    data['PFTOTAL'] = data['PFTOTAL']/1000.0
    data['date'] = m[0]
    data.to_csv('F:\\DataWarehouse\\office\\%s.csv' % m[0],index=False)
    return m[0]


def cassandra(m):
    data = pd.read_sql(branchYSQuery % (m[0],m[1]), con=con)
    data['YSTOTAL'] = data['YSTOTAL']/1000.0
    data['date'] = m[0]
    data.to_csv('F:\\DataWarehouse\\asmaa\\%s.csv' % m[0],index=False)
    return m[0]

def irene(m):
    time.sleep(randint(0,9))
    return m

#pd.date_range(date(2016,1,1),date(2017,11,30))

if __name__ == '__main__':
    with Pool(processes=8) as pool:
        for i in pool.imap_unordered(cassandra,month):
            print(i)



def dataMerge():
    for i in pd.date_range(date(2016,1,1),date(2017,11,30)):
         d = i.strftime('%Y-%m-%d')
         print(d)
         
         data = pd.read_csv('F:\\Data\\FactWithCost\\%s.csv' % d, encoding='gbk',dtype={'CUSTCODE':object,'PLUCODE':object})
         if i.strftime('%Y-%m-%d')==date(2016,1,1).strftime('%Y-%m-%d'):
             data.to_csv('F:\\Data\\FactWithCost\\workingData.csv',mode='w',index=False)
         else:
             data.to_csv('F:\\Data\\FactWithCost\\workingData.csv',mode='a',header=False, index=False)





