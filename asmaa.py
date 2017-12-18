from multiprocessing import Pool, TimeoutError
import pandas as pd
import numpy as np
import time
import os
import cx_Oracle as oracle
from random import randint
from datetime import timedelta, date
import dask.dataframe as dd


con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')

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


coverRateDCQuery = """
 Select p.materialcode,
       c.channel,
       c.mergeAfter,
       c.remark,
       s.rzdate,
       s.counts,
       s.pftotal,
       round(s.counts*e.lsavghjprice*1000,0) as COGS
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
       ) s,
  officemember@finance c, tskuplu p,tskuetpparas e,torgdrprelation r
 Where s.branchno = c.orgcode 
   And s.customercode = c.custcode 
   And s.plucode = p.plucode 
   And s.branchno = r.orgcode
   And r.preorgcode = e.orgcode And e.ismainetp = '1' And p.pluid = e.pluid
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')"""

def get_DCDetail():
    data = dd.read_csv('F:\\Data\\FactWithCost\\20*.csv',encoding='gbk',dtype={'CUSTCODE':object,'PLUCODE':object})
    return data
   
def get_HQDetail():
    data = dd.read_csv('F:\\Data\\HQFact\\HQ20*.csv',encoding='gbk',dtype={'ORGCODE':object,'PLUID':object})


def HQdate():
    res = data[(data['date']>='2017-11-01') & (data['date']<='2017-12-10')].merge(sku,on='PLUID').compute()
    irene = res.groupby('MATERIALCODE').agg({'PSCOUNT':lambda x:x.sum(),'PSTOTAL':lambda x:x.sum()})

def headquarterETL(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(headQuarterQuery % (dd,dd,dd,dd),con=con)
    data['date'] = dd
    data.to_csv('F:\\Data\\HQFact\\HQ%s.csv' % dd,index=False)
    return dd


def default():
    fact = pd.read_csv('F:\\Data\\fact\\workingData.csv',encoding='gbk',dtype={'CUSTOMERCODE':object})
    fact = fact.rename(columns={'BRANCHNO':'ORGCODE','CUSTOMERCODE':'CUSTCODE'})
    cust = pd.read_csv('F://Data/CustList.csv',encoding='gbk',dtype={'CUSTCODE':object,'KID':object,'FGSORGCODE':object})
    custFact = fact.merge(cust,how='left',on=['ORGCODE','CUSTCODE'])
    custFact = custFact[custFact['CHANNEL'].notnull()]
    sku = pd.read_csv('F:\\Data\\sku.csv',encoding='gbk',dtype={'PLUID':object,'CLSCODE':object,'CLSID':object})
    skuCustFact = custFact.merge(sku,how = 'left', on='PLUCODE')
    


def get_branchDetail():
    data = dd.read_csv('F:\\Data\\BranchFact\\20*.csv',encoding='gbk',dtype={'ORGCODE':object,'PLUID':object})
    return data

        
def calculation(factData,skuData,custData):
    data = factData.merge(custData,how='left',on=['ORGCODE','CUSTCODE'])
    data = data[data['CHANNEL'].notnull()]
    data = data.merge(skuData[['PLUNAME','MATERIALCODE']],how='left', on='PLUCODE')
    return data.groupby(['PLUCODE','PLUNAME','MATERIALCODE','FGSORGCODE','FGSORGNAME','POSTCODE'])



    
def dump(file):
    file.to_csv('F:\\Data\\dump.csv')
   
def multiProcessQuery(f,p):
    with Pool(processes=2) as pool:
        for i in pool.imap_unordered(f,p):
            print(i)
            
    
    
            
def coverRateETL(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_sql(coverRateDCQuery % (dx,dx,dx,dx),con=con)
    data['COGS'] = data['COGS']/1000.0
    data.to_csv('F:\\Data\\coverRate\\DC%s.csv' % dd,index=False)
    return dd


#multiProcessQuery(headquarterETL,pd.date_range(date(2016,1,1),date(2017,12,8)))
    

def coverRateReport():
    
    print('0.3')
    data = dd.read_csv('F:\\Data\\coverRate\\DC20*.csv',encoding='gbk')
    data['MD'] = data['MERGEAFTER'] + data['RZDATE']
    
    data2017  = data[(data['RZDATE'] >= '2017-01-01') & (data['RZDATE'] <= '2017-11-30')].compute()
    print('17 loaded')
    res17 = data2017.groupby(['MATERIALCODE','CHANNEL'],as_index=False).agg({'MD':lambda x:x.nunique(),'COGS':lambda x:x.sum(),'COUNTS':lambda x:x.sum(),'MERGEAFTER':lambda x:x.nunique()})
    a = float(pd.read_sql("""select count(distinct mergeafter) from officemember@finance where remark like '%%有效%%' and channel = '传统'""",con=con).iloc[0,0])
    b = float(pd.read_sql("""select count(distinct mergeafter) from officemember@finance where remark like '%%有效%%' and channel = '办公'""",con=con).iloc[0,0])
    c = float(pd.read_sql("""select count(distinct mergeafter) from officemember@finance where remark like '%%有效%%' and channel = '精品'""",con=con).iloc[0,0])
    res17['覆盖率'] = res17['MERGEAFTER']/a
    res17['覆盖率'][res17['CHANNEL'] == '办公'] = res17['MERGEAFTER']/b
    res17['覆盖率'][res17['CHANNEL'] == '精品'] = res17['MERGEAFTER']/c
    res17['复购率'] = res17['MD']/res17['MERGEAFTER'] - 1.0
    res17['year'] = '2017'
    
    
    data2016  = data[(data['RZDATE'] >= '2016-01-01') & (data['RZDATE'] <= '2016-11-30')].compute()
    print('16 loaded')
    res16 = data2016.groupby(['MATERIALCODE','CHANNEL'],as_index=False).agg({'MD':lambda x:x.nunique(),'COGS':lambda x:x.sum(),'COUNTS':lambda x:x.sum(),'MERGEAFTER':lambda x:x.nunique()})
    x = float(pd.read_sql("""select count(distinct mergeafter) from officemember@finance where remark like '%%有效1601%%' and channel = '传统'""",con=con).iloc[0,0])
    y = float(pd.read_sql("""select count(distinct mergeafter) from officemember@finance where remark like '%%有效1601%%' and channel = '办公'""",con=con).iloc[0,0])
    z = float(pd.read_sql("""select count(distinct mergeafter) from officemember@finance where remark like '%%有效1601%%' and channel = '精品'""",con=con).iloc[0,0])
    
    res16['覆盖率'] = res16['MERGEAFTER']/x
    res16['覆盖率'][res16['CHANNEL'] == '办公'] = res16['MERGEAFTER']/y
    res16['覆盖率'][res16['CHANNEL'] == '精品'] = res16['MERGEAFTER']/z
    res16['复购率'] = res16['MD']/res16['MERGEAFTER'] - 1.0
    res16['year'] = '2016'
    
    res = pd.concat([res16,res17])
    
    res.to_csv('F:\\Data\\coverRate.csv')
    
    
def coverRateAlternative():
    cust = pd.read_sql("""select orgcode,custcode,mergeafter,channel from officemember@finance where remark like '%有效%'""",con=asmaa.con)
    sku = pd.read_csv('F://Data//working/17128sku-1.csv',dtype={'PLUID':object,'PLUCODE':object})
    irene = data[(data['RZDATE'] >= '2017-11-01') & (data['RZDATE'] <= '2017-11-30')].merge(sku,on='PLUCODE').merge(cust,on=['ORGCODE','CUSTCODE']).compute()
    
    irene['MD'] = irene['RZDATE'] + irene['MERGEAFTER']
    
    aa = irene.groupby(['MATERIALCODE','CHANNEL'],as_index=False).agg({'MD':lambda x:x.nunique(),'COSTOFGOODSSOLD':lambda x:x.sum(),'COUNTS':lambda x:x.sum(),'MERGEAFTER':lambda x:x.nunique()})
    aa['averageStore'] = aa['COSTOFGOODSSOLD']/aa['MERGEAFTER']
    
#data = factData(date(2017,1,1),date(2017,10,31))
#result = calculation(factData(date(2017,10,1),date(2017,10,31)),skuData(),custData())
    
def branchData():
    dcCust = dcCust.rename(columns={'CUSTCODE':'ETPCODE'})
    irene = data[(data['date'] >= '2017-01-01') & (data['date'] <= '2017-11-30')].merge(sku[['PLUID','LANE2017']],on='PLUID').merge(dcCust[['ORGCODE','ETPCODE','MERGEAFTER','CHANNEL']],on=['ORGCODE','ETPCODE'])
    
    bd = get_branchDetail()
    sku = pd.read_csv('F://Data//working/17128sku-1.csv',dtype={'PLUID':object,'PLUCODE':object})
    dc = pd.read_sql("""select etpename,etpcode,postcode from tetpenterprise where postcode in ('三级','二级','一级')""",con=con)
    res = bd.merge(sku,on='PLUID').merge(dc,on='ETPCODE')
    res = res[(res['date']>='2017-11-01') & (res['date']<='2017-12-10')].compute()
    a = res[res['POSTCODE'] != '一级'].groupby('PLUID').ETPENAME.nunique()
    a = a.reset_index()
    b = res[res['POSTCODE'] == '一级'].groupby('PLUID').ETPENAME.nunique()
    b = b.reset_index()
    return a.merge(b,on='PLUID')
    


def retailData():
    irene = data[(data['RZDATE'] >= '2016-01-01') & (data['RZDATE'] <= '2016-11-30')].merge(sku,on='PLUCODE').merge(cust[['CUSTCODE','ORGCODE','MERGEAFTER','CHANNEL']],on=['ORGCODE','CUSTCODE'])
    irene.groupby(['MERGEAFTER','CHANNEL','LANE2017'])['COSTOFGOODSSOLD'].sum().compute()

    dcCust = pd.read_sql("""select orgcode,custcode,mergeafter,channel from officemember@finance where remark like '%大仓%'""",con=asmaa.con)
    data = dd.read_csv('F:\\Data\\BranchFact\\20*.csv',encoding='gbk',dtype={'ORGCODE':object,'PLUID':object})

    irene = data[(data['RZDATE'] >= '2016-01-01') & (data['RZDATE'] <= '2016-11-30') & (data['ORGCODE'] == 'C27001') & (data['CUSTCODE'] == '817083')]
    
    
def hqData():
    asmaa = pd.read_excel('F:\\Data\\branchDetail.xlsx',encoding='gbk',sheetname='data',dtype={'主客户编码':object,'客户编码':object})
    asmaa = asmaa.rename(columns={'主客户编码':'majorCode','客户编码':'ORGCODE','公司名称':'ORGNAME'})