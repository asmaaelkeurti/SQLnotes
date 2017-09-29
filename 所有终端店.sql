--spool "E:\load from oracle\asmaa.csv"

Select 
       --c.orgcode,
       c.custcode,
       --c.kid,
       --p.udp14 As 品牌,
       --f.postcode As etplevel,
       Sum(s.counts*e.lsavghjprice)/10000 As 成本,
       c.isbangong,
       c.isjingpin,
       c.isxuesheng
  From (
Select h.branchno,h.customercode,b.plucode,b.counts
  From tmgpfsaledetail_rpt@hsrpt b,tmgpfsaleform_rpt@hsrpt h
 Where b.branchno = h.branchno And b.pfsaleno = h.pfsaleno
   And h.rzdate Between '2017-01-01' And '2017-09-20'
Union All
Select h.branchno,h.customercode,b.plucode,-b.counts As counts
  From tmgpfreturndetail_rpt@hsrpt b,tmgpfreturnform_rpt@hsrpt h
 Where b.branchno = h.branchno And b.pfreturnno = h.pfreturnno
   And h.rzdate Between '2017-01-01' And '2017-09-20'
       ) s,tdrpetpcustdetail c,torgdrprelation r,tskuplu p,tskuetpparas e,TETPENTERPRISE f
 Where s.branchno = c.orgcode 
   And s.customercode = c.custcode 
   And s.branchno = r.orgcode 
   --and c.isbangong='1' or c.isjingpin='1' or c.isxuesheng='1'
   And s.plucode = p.plucode And r.preorgcode = e.orgcode And p.pluid = e.pluid
   and s.branchno=f.etpcode
   And Exists(Select 1 From torgdrpdisp Where orgcode = s.branchno And remark Like '%有效%')  --有效配送中心
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%' And clscode Not Like '011%')  --晨光类
   And e.ismainetp = '1'
 Group By c.custcode, c.isbangong, c.isjingpin, c.isxuesheng;
 
--spool off;