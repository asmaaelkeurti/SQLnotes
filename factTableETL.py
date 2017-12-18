from multiprocessing import Pool, TimeoutError
import pandas as pd
import time
import os
import cx_Oracle as oracle
from random import randint
from datetime import timedelta, date
import psycopg2



pgContext = "dbname='postgres' user='postgres' host='localhost' password='lwj380279011'"



con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
factQuery = """select s.* from (
Select h.rzdate,h.branchno as orgcode,h.customercode as custcode,b.plucode,b.counts,b.pftotal,b.ystotal
  From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select h.rzdate,h.branchno as orgcode,h.customercode as custcode,b.plucode,-b.counts As counts, -b.pftotal as pftotal, -b.yttotal as ystotal
  From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s') s, tskuplu p, tcatcategory c
where s.plucode = p.plucode and p.clsid = c.clsid
   and c.clscode like '0%%'
"""

newFactQuery = """
Select s.rzdate,s.branchno as orgcode, s.customercode as custcode, s.plucode,s.counts,s.pftotal,round(s.counts*e.lsavghjprice*1000,0) as costofgoodssold
  From (
Select h.rzdate,h.branchno,h.customercode,b.plucode,b.counts,b.pftotal
  From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select h.rzdate,h.branchno,h.customercode,b.plucode,-b.counts As counts, -b.pftotal as pftotal
  From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s'
       ) s 
       left join torgdrprelation r on s.branchno = r.orgcode
       left join tskuplu p on s.plucode = p.plucode
       left join tskuetpparas e on r.preorgcode = e.orgcode and p.pluid = e.pluid and e.ismainetp='1' 
       inner join tcatcategory c on p.clsid = c.clsid and c.clscode like '0%%'
"""

branchQuery = """select H.OrgCode,
                        H.EtpCode,
                        S.pluid,
                        round(sum(S.XsCount)*1000,0) XsCount,
                        round(sum(S.HJCost)*1000,0) HJCost,
                        round(sum(S.HxTotal)*1000,0) HxTotal
from tSalPluDetail%s S,tstkkcjzhead%s K,tWslXsHead H, tcatcategory c, tskuplu p
where S.KcBillno=K.billno and K.YWBILLNO=h.billno and S.pluid=p.pluid and p.clsid = c.clsid
and to_char(h.jzdate,'yyyy-mm-dd') between  '%s' and '%s' 
and c.clscode like '0%%'
group by H.OrgCode,H.EtpCode, S.pluid
"""

headQuarterQuery = """
Select s.orgcode,
       p.pluid,
       pstotal,
       pscount
  From (
Select h.shorgcode As orgcode,b.plucode,b.pstotal,b.pscount
  From tdstpsbody b,tdstpshead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '%s' And '%s'
   And h.orgcode In('1','2','5')
   and h.depid <> '10010000000023'
Union All
Select h.thorgcode As orgcode,b.plucode,-b.thtotal As pstotal, -b.thcount As pscount
  From tdstrtnbody b,tdstrtnhead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '%s' And '%s'
   And h.orgcode In('1','2','5')
   and h.depid <> '10010000000023'
       ) s,tskuplu p
 Where s.plucode = p.plucode
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
"""

def newQuery():
    for i in pd.date_range(date(2017,10,1),date(2017,11,30)):
        d = i.strftime('%Y-%m-%d')
        print(d)
        data = pd.read_sql(newFactQuery % (d,d,d,d),con=con)
        data['COSTOFGOODSSOLD'] = data['COSTOFGOODSSOLD']/1000.0
        data.to_csv('F:\\Data\\FactWithCost\\%s.csv' % d,index=False)
        
def query():
    for i in pd.date_range(date(2017,1,1),date(2017,11,20)):
        d = i.strftime('%Y-%m-%d')
        print(d)
        data = pd.read_sql(factQuery % (d,d,d,d),con=con)
        data.to_csv('F:\\Data\\fact\\%s.csv' % d,index=False)
        
def f(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(newFactQuery % (dd,dd,dd,dd),con=con)
    data['COSTOFGOODSSOLD'] = data['COSTOFGOODSSOLD']/1000.0

    data.to_csv('F:\\Data\\FactWithCost\\%s.csv' % dd,index=False)

    return dd

def branch(d):
    f = d.strftime('%Y%m')
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(branchQuery % (f,f,dd,dd),con=con)
    if len(data) != 0:
        data['date'] = dd
        data['XSCOUNT'] = data['XSCOUNT']/1000.0
        data['HJCOST'] = data['HJCOST']/1000.0
        data['HXTOTAL'] = data['HXTOTAL']/1000.0
        data.to_csv('F:\\Data\\BranchFact\\%s.csv' % dd,index=False)
    return dd

def irene(m):
    time.sleep(randint(0,9))
    return m

def headquarterETL(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(headQuarterQuery % (dd,dd,dd,dd),con=con)
    if(len(data)>0):
        data['date'] = dd
        data.to_csv('F:\\Data\\HQFact\\HQ%s.csv' % dd,index=False)
    return dd

def endDetailETL(startDate):
    runQuery(["""delete from e where rzdate >= '%s'""" % startDate.strftime('%Y-%m-%d')])
    print('clean done')
    
    for i in pd.date_range(startDate,date(int(time.strftime('%Y')),int(time.strftime('%m')),int(time.strftime('%d')))):
        while True:
            try:
                i = print(endDetailPGinsert(i))
                break
            except:
                time.sleep(10)
                print('retry')



def endDetailPGinsert(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_csv('F:\\Data\\FactWithCost\\%s.csv' % dd,encoding='gbk',dtype={'CUSTCODE':object,'PLUCODE':object})
    data = data.fillna(0)
    pgconn = psycopg2.connect(pgContext)
    cur = pgconn.cursor()
    
    copy_sql = """COPY e FROM stdin WITH CSV HEADER"""
    
    with open('F:\\Data\\FactWithCost\\%s.csv' % dd, 'r') as f:
        cur.copy_expert(sql=copy_sql,file=f)
        pgconn.commit()
        cur.close()

    pgconn.close()
    
    return dd

def runQuery(qList):
    for i in qList:
        pgconn = psycopg2.connect(pgContext)
        cur = pgconn.cursor()
        cur.execute(i)
        pgconn.commit()
        cur.close()
        pgconn.close()
        print(i)

#    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
#    with Pool(processes=16) as pool:
#        for i in pool.imap_unordered(branch,pd.date_range(date(2016,1,1),date(2017,12,6))):
#            print(i)

def run(f, p,NofProcesses=16):
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    with Pool(processes=16) as pool:
        for i in pool.imap_unordered(f,p):
            print(i)

#__spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"   
    with Pool(processes=8) as pool:
        for i in pool.imap_unordered(headquarterETL,pd.date_range(date(2016,1,1),date(int(time.strftime('%Y')),int(time.strftime('%m')),int(time.strftime('%d'))))):
            print(i)
            
    with Pool(processes=8) as pool:
        for i in pool.imap_unordered(branch,pd.date_range(date(2016,1,1),date(int(time.strftime('%Y')),int(time.strftime('%m')),int(time.strftime('%d'))))):
            print(i)
##            
    with Pool(processes=4) as pool:
        for i in pool.imap_unordered(f,pd.date_range(date(2016,1,1),date(int(time.strftime('%Y')),int(time.strftime('%m')),int(time.strftime('%d'))))):
            print(i)
#            
    endDetailETL(date(2016,1,1))
    runQuery(['create index orgcode_index on e (orgcode)','create index plucode_index on e (plucode)','create index custcode_index on e (custcode)','create index rzdate_index on e (rzdate)'])