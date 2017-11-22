import cx_Oracle as oracle
import pandas as pd

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
month = [   
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
Select c.kid,
       c.orgcode,
       c.custcode,
       round(sum(s.counts*e.lsavghjprice)*1000,0) as CopyPaperCOGS,
       count(distinct s.plucode) as CopyPaperUniqueSKU
  From (
Select h.rzdate,h.branchno,h.customercode,b.plucode,b.counts,b.pftotal,b.ystotal
  From tmgpfsaledetail_rpt b,tmgpfsaleform_rpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '%s' And '%s'
Union All
Select NULL as rzdate,h.branchno,h.customercode,b.plucode,-b.counts As counts, -b.pftotal as pftotal, -b.yttotal as ystotal
  From tmgpfreturndetail_rpt b,tmgpfreturnform_rpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '%s' And '%s'
       ) s,
  tdrpetpcustdetail c,
  torgdrprelation r,tskuplu p,tskuetpparas e,tcatcategory a
 Where s.branchno = c.orgcode 
   And s.customercode = c.custcode 
   And s.branchno = r.orgcode
   And s.plucode = p.plucode And r.preorgcode = e.orgcode And p.pluid = e.pluid
   and p.clsid = a.clsid
   And Exists(Select 1 From torgdrpdisp Where orgcode = s.branchno And remark Like '%%有效%%')  --有效配送中心
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
   And e.ismainetp = '1'
   and a.clscode = '0100101'
 Group By c.kid,c.orgcode,c.custcode
"""

def asmaa():
    for m in month:
        print(m)
        data = pd.read_sql(query % (m[0],m[1],m[0],m[1]),con=con)

        data['COPYPAPERCOGS'] = data['COPYPAPERCOGS']/1000.0
        data['date'] = m[0]
    
        if m[0] == month[0][0]:
            data.to_csv('C:\\Users\\150972\\Desktop\\working\\office\\copypaper.csv',index=False)
        else:
            data.to_csv('C:\\Users\\150972\\Desktop\\working\\office\\copypaper.csv',mode='a',header=False, index=False)

            
asmaa()






























