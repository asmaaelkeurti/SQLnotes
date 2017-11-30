from multiprocessing import Pool, TimeoutError
import pandas as pd
import time
import os
import cx_Oracle as oracle
from random import randint
from datetime import timedelta, date


con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
month = [   
            ['2017-11-01','2017-11-30'],
            ['2017-10-01','2017-10-31'],
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
            ['2016-01-01','2016-01-31']
        ]

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

branchQuery = """
select H.OrgCode,H.EtpCode,S.pluid,sum(S.XsCount),sum(S.HJCost),sum(S.HxTotal)
from tSalPluDetail201711 S,tstkkcjzhead201711 K,tWslXsHead H, tcatcategory c, tskuplu p
where S.KcBillno=K.billno and K.YWBILLNO=h.billno and S.pluid=P.pluid and P.clsid = C.clsid
and to_char(h.jzdate,'yyyy-mm-dd') between  '2017-11-28' and '2017-11-28' 
and c.clscode like '0%'
group by H.OrgCode,H.EtpCode, S.pluid"""

def f(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(query % (dd,dd,dd,dd),con=con)
    data.to_csv('F:\\DataWarehouse\\salesDetail\\%s.csv' % dd,index=False)
    return dd

def asmaa(m):
    time.sleep(randint(0,9))
    return m

if __name__ == '__main__':
    with Pool(processes=8) as pool:
        for i in pool.imap_unordered(f,pd.date_range(date(2016,1,1),date(2017,11,30))):
            print(i)
