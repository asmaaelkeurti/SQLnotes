import cx_Oracle as oracle
import pandas as pd

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
month = [   
            #['2017-11-01','2017-11-30'],
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
Select c.custname,
       c.kid,
       n.lane2017,
       round(sum(s.counts*e.lsavghjprice)*1000,0) as costOfGoodsSold,
       round(sum(s.counts)*1000,0) totalcounts,
       round(sum(s.pftotal)*1000,0) pftotal
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
  (select orgcode,custcode,custname,kid,isbangong,isxuesheng,isjingpin,
(case   when isjingpin = '1' THEN '精品渠道'
        when isbangong = '1' THEN '办公渠道'
        when isxuesheng = '1' THEN '传统渠道'
    END) channel
from tdrpetpcustdetail where isjingpin = '1' or isbangong='1' or isxuesheng='1') c,
  torgdrprelation r,tskuplu p,tskuetpparas e,TETPENTERPRISE f,torgdrprelation fgs,notSigned2018@finance n
 Where s.branchno = c.orgcode 
   And s.customercode = c.custcode 
   And s.branchno = r.orgcode
   And s.plucode = p.plucode And r.preorgcode = e.orgcode And p.pluid = e.pluid
   and s.branchno=f.etpcode
   And Exists(Select 1 From torgdrpdisp Where orgcode = s.branchno And remark Like '%%有效%%')  --有效配送中心
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
   And e.ismainetp = '1'
   And r.preorgcode = fgs.orgcode
   And s.plucode = n.plucode
 Group By c.custname,c.kid,n.lane2017
"""

skuQuery = """select p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,sd.sdtotal,IPO.listDate,
    n.Lane2018,n.Lane2017,
    (case when f.evaluationname IS NOT NULL THEN f.evaluationname
          when e.clsname IS NOT NULL THEN '考试项目'
          when p.functionality = 'C' THEN '考试项目'
          when p.highlevel = '物料' THEN '物料'
          else '其它品类'
    END) adjusted
    from
(select p.pluid, p.plucode, p.pluModel, p.MaterialCode, p.pluname, p.LRDate, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp9 functionality, p.udp14 flag
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
        ) p 
        left join notSigned2018@finance n
            on p.MaterialCode = n.MaterialCode and p.PLUCODE = n.PLUCODE
        left join fanxu@finance f 
            on p.lowLevel = f.clsname
        left join otherExamItemList@finance e
            on p.plucode = e.plucode
        left join (select  p.MATERIALCODE,
                    max(b.price) as price
                    from tPrcPsJgzcHead h, tPrcPsJgzcBody b, tskuplu p
                    where h.jgzccode = b.jgzccode
                    and currcode = '0' 
                    and b.plucode = p.plucode
                    and length(h.orgcode) = 1 group by p.MATERIALCODE
          ) price on p.materialcode = price.materialcode
        left join (
            select sum(s.plancount*s.psprice) sdTotal,p.materialcode
            from tdstpsplanexe s, tskuplu p
            where s.billtype = 4
            and s.depid<>10010000000023
            and s.pluid = p.pluid
            group by p.materialcode
        )sd on p.materialcode = sd.materialcode 
        left join (
            select b.materialcode,min(h.jzdate) listDate
            from tdstpshead h, tdstpsbody b
            where h.billno = b.billno
                and h.orgcode in ('1', '2') --h.depid in ('10010000000021','10010000000022')  
                and h.depid <> '10010000000023' --部门
            group by b.materialcode
        )IPO on IPO.materialcode = p.materialcode
        group by p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,sd.sdtotal,IPO.listDate,
                n.Lane2018,n.Lane2017"""


headQuarterQuery = """
Select s.orgcode,
       p.plucode,
       p.materialcode,
       round(sum(s.pstotal)*1000,0) pstotal,
       round(sum(s.pscount)*1000,0) pscount
  From (
Select h.shorgcode As orgcode,b.plucode,b.pstotal,b.pscount
  From tdstpsbody b,tdstpshead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '%s' And '%s'
   And h.orgcode In('1','2')
   and h.depid <> '10010000000023'
Union All
Select h.thorgcode As orgcode,b.plucode,-b.thtotal As pstotal, -b.thcount As pscount
  From tdstrtnbody b,tdstrtnhead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '%s' And '%s'
   And h.orgcode In('1','2')
   and h.depid <> '10010000000023'
       ) s,tskuplu p
 Where s.plucode = p.plucode
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')  --晨光类
 Group By s.orgcode, p.plucode,p.materialcode
"""

branchQuery = """select fgscode,etpcode,plucode, materialcode,round(sum(cbtotal)*1000,0) cbtotal
from(
  select        
                  h.OrgCode fgscode, 
                  h.etpcode etpcode,
                  c.plucode plucode,
				  c.materialcode materialcode,
                 (Case h.billtype When '0' Then b.pfcount Else -b.pfcount End)*e.lsavghjprice As cbtotal
from twslxshead h, twslxsBody b,tskuplu c,tcatcategory d,tskuetpparas e
where     h.billno = b.billno
      and c.clsid =  d.clsid
      and c.plucode = b.plucode
      and h.orgcode not in('054')
      and d.clscode like '0%%'
      and d.clscode not like '011%%'
      and h.jzdate is not null
      And h.orgcode = e.orgcode And c.pluid = e.pluid and e.ismainetp = '1'
      and to_char(h.jzdate,'yyyy-mm-dd') between '%s' and '%s')
group by fgscode,etpcode,plucode, materialcode
"""

def asmaa():
    for m in month:
        print(m)
        data = pd.read_sql(headQuarterQuery % (m[0],m[1],m[0],m[1]),con=con)

        data['PSTOTAL'] = data['PSTOTAL']/1000.0
        data['PSCOUNT'] = data['PSCOUNT']/1000.0
        data['date'] = m[0]
    
        if m[0] == month[0][0]:
            data.to_csv('C:\\Users\\150972\\Desktop\\working\\8x4\\HeadQuarterMonthFacts.csv',index=False)
        else:
            data.to_csv('C:\\Users\\150972\\Desktop\\working\\8x4\\HeadQuarterMonthFacts.csv',mode='a',header=False, index=False)

def sku():
    skuData = pd.read_sql(skuQuery,con=con)
    skuData.to_csv('F:\\DataWarehouse\\sku.csv',index=False)
    
def branch():
    for m in month:
        print(m)
        data = pd.read_sql(branchQuery % (m[0],m[1]),con=con)

        data['CBTOTAL'] = data['CBTOTAL']/1000.0
        data['date'] = m[0]
    
        if m[0] == month[0][0]:
            data.to_csv('F:\\DataWarehouse\\8x4\\BranchMonthFacts.csv',index=False)
        else:
            data.to_csv('F:\\DataWarehouse\\8x4\\BranchMonthFacts.csv',mode='a',header=False, index=False)
            
branch()






























