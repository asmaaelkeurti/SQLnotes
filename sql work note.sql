select d.orgcode, d.custcode, d.custname, d.address, e.etpname, e.etpcode, e.cetpname, e.postcode,e.email
from TDRPETPCUSTDETAIL d,TETPENTERPRISE e
where d.Orgcode = e.etpcode and d.isperfect = '1' and d.isxuesheng = '1';

select BranchNo, BranchName, PFsaleno, orgcode,orgname,rzdate, customercode, customername, deliveraddr, ystotal, counts from TMGPFSALEFORM_RPT
where rzdate > '2017-06-01'

select f.BranchNo, f.BranchName, f.PFsaleno, f.orgcode,f.orgname,f.rzdate, f.customercode, f.customername, f.deliveraddr, f.ystotal f_ystotal, f.counts f_counts,
        d.serialno, d.plucode, d.jprice, d.PFPRICE, d.PRICE, d.COUNTS, d.pftotal,d.ystotal
from TMGPFSALEFORM_RPT f, TMGPFSALEDETAIL_RPT d
where f.rzdate > '2017-06-01' 
  and f.pfsaleno = d.pfsaleno 
  and f.branchno = d.branchno;
  
select p.pluid, p.plucode, p.MaterialCode,p.pluname, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp14 flag   
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
		and substr(a.clscode,1,3) <> 011;


select f.BranchNo, f.BranchName, f.PFsaleno, f.orgcode,f.orgname,f.rzdate, f.customercode, f.customername, f.deliveraddr, f.ystotal f_ystotal, f.counts f_counts,
        d.serialno, d.plucode, d.jprice, d.PFPRICE, d.PRICE, d.COUNTS, d.pftotal,d.ystotal,
        plu.pluid, plu.materialcode, plu.pluname, plu.clscode, plu.product, plu.highlevel, plu.midlevel, plu.lowlevel, plu.flag
from TMGPFSALEFORM_RPT f, TMGPFSALEDETAIL_RPT d, 
    (select p.pluid, p.plucode, p.MaterialCode,p.pluname, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp14 flag   
        from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
    where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
		and substr(a.clscode,1,3) <> 011) plu
where f.rzdate > '2017-06-01' 
  and f.pfsaleno = d.pfsaleno 
  and f.branchno = d.branchno
  and d.plucode = plu.plucode;



select  f.BranchNo, f.BranchName, f.PFsaleno, f.orgcode,f.orgname,f.rzdate, f.customercode, f.customername, f.deliveraddr, f.ystotal f_ystotal, f.counts f_counts,
        d.serialno, d.plucode, d.jprice, d.PFPRICE, d.PRICE, d.COUNTS, d.pftotal,d.ystotal,
        plu.pluid, plu.materialcode, plu.pluname, plu.clscode, plu.product, plu.highlevel, plu.midlevel, plu.lowlevel, plu.flag,
        c.custcode, c.custname, c.address,c.orgcode,
        e.etpname, e.etpcode, e.cetpname, e.postcode, e.email
from TMGPFSALEFORM_RPT f, TMGPFSALEDETAIL_RPT d, TDRPETPCUSTDETAIL c, TETPENTERPRISE e,
    (select p.pluid, p.plucode, p.MaterialCode,p.pluname, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp14 flag   
        from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
    where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
		and substr(a.clscode,1,3) <> 011) plu
where f.rzdate > '2017-09-10' 
  and f.pfsaleno = d.pfsaleno 
  and f.branchno = d.branchno
  and d.plucode = plu.plucode
  and f.customercode = c.custcode
  and f.BranchNo = e.etpcode;








select hz.Zbcode, 
       hz.PluCode, 
       hz.pluname, 
       hz.MATERIALCODE,
       hz.CARGONO,
       hz.postcode,
       hz.PLUMODEL,
       hz.clscode, 
       hz.clsname,  
       count(distinct hz.preorgcode) FgsCounts,
       count(distinct hz.ORGCODE) BraCounts,
       count(distinct hz.PFSaleNo) PFSaleCounts, 
       count(distinct hz.CUSTCODE) cutcounts,
       count(distinct hz.Rzdate) RzCounts,
       count(distinct hz.PluCode) PluCounts,
       sum(hz.Counts) Totalcounts,
       sum(hz.ystotal) Totalys,
       sum(hz.PFTotal) PFTotal
  from 
        (Select g.preorgcode,
               i.ORGCODE, 
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               i.CUSTCODE || i.ORGCODE CUSTCODE, 
               f.Rzdate || i.CUSTCODE || i.ORGCODE Rzdate,
               f.PFSaleNo,
               en.postcode,
               d.PluCode, 
               t.pluname, 
               t.MATERIALCODE,
               t.CARGONO,
               t.PLUMODEL,
               p.clscode, 
               p.clsname, 
               d.Counts,
               d.ystotal,
               d.PFTotal
          From tSkuplu                  t,
               tCatCategory              p, 
               torgdrpdisp               h,  
               tOrgDrpRelation           g, 
               tDrpEtpCustform           i, 
               tDrpEtpCustdetail         e,
               tmgpfsaleform_rpt@hsrpt   f,
               tmgpfsaledetail_rpt@hsrpt d,
               tEtpEnterprise            en
         Where f.PFSaleNo = d.pfsaleno and f.branchno = d.branchno 
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode 
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid  
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%完美有效%' 
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1' 
           and f.branchno = en.etpcode
           and f.Rzdate between '2017-09-01' and '2017-09-10'
        union all
        Select ''             as preorgcode, 
               ''             as ORGCODE, 
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               ''             as CUSTCODE, 
               ''             as Rzdate,   
               ''             as PFSaleNo,
               en.postcode,
               d.PluCode, 
               t.pluname, 
               t.MATERIALCODE,
               t.CARGONO,
               t.PLUMODEL,
               p.clscode, 
               p.clsname, 
               -d.Counts      as Counts,
               -d.yttotal     as ystotal,
               -d.PFTotal     as PFTotal
          From tSkuplu                    t,
               tCatCategory                p, 
               torgdrpdisp                 h, 
               tOrgDrpRelation             g, 
               tDrpEtpCustform             i, 
               tDrpEtpCustdetail           e,
               tmgpfreturnform_rpt@hsrpt   f,
               tmgpfreturndetail_rpt@hsrpt d,
               tEtpEnterprise              en
         Where f.PFreturnNo = d.pfreturnno and f.branchno = d.branchno
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode  
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid 
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%完美有效%'
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1'
           and f.branchno = en.etpcode
           and f.Rzdate between '2017-09-01' and '2017-09-10') hz
        Group By Zbcode, PluCode, pluname, postcode, clscode, clsname, MATERIALCODE, CARGONO, PLUMODEL;





select a.Zbcode,   
       a.Precounts,   
       a.Orgcounts,   
       a.Kidcounts,   
       (a.Precounts - x.FgsCounts) NoSFCounts, 
       (a.Orgcounts - x.BraCounts) NoSOCounts, 
       (a.Kidcounts - x.cutcounts) NoSSCounts, 
       x.PFSaleCounts,  
       x.FgsCounts, 
       x.BraCounts, 
       x.cutcounts,  
       x.RzCounts, 
       x.postcode,
       nvl(x.PluCode,'无记录')PluCode,  
       nvl(x.pluname,'无记录')pluname, 
       nvl(x.MATERIALCODE,'无记录')MATERIALCODE, 
       nvl(x.CARGONO,'无记录')CARGONO, 
       nvl(x.PLUMODEL,'无记录')PLUMODEL, 
       --(select remark1 from hscmp.rpt_skulist where PluCode= x.PluCode) PluRemark1,    
       --(select remark2 from hscmp.rpt_skulist where PluCode= x.PluCode) PluRemark2, 
       nvl(x.clscode,'无记录')clscode, 
       nvl(x.clsname,'无记录')clsname, 
       case x.RzCounts when 0 then 0 else round(x.Totalcounts/x.RzCounts) end ACC,  
       x.Totalcounts,  
       x.Totalys,  
       x.PFTotal 
from (
    select hz1.Zbcode, 
       count(distinct hz1.preorgcode) Precounts, 
       count(distinct hz1.ORGCODE) Orgcounts, 
       count(distinct hz1.Kid) Kidcounts 
    from 
      (select g.preorgcode,  
              (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
              i.ORGCODE,  
              i.kid   
         From tDrpEtpCustform           i, 
              tDrpEtpCustdetail         e, 
              tOrgDrpRelation           g
        Where e.kid = i.kid
          and i.Orgcode = g.Orgcode  
          and e.ORGCODE in (select ORGCODE from torgdrpdisp where remark like '%完美有效%')      
          and e.ISPERFECT = '1'
          and e.isxuesheng = '1') hz1 
    Group by Zbcode
)   a
left join
    (
    select hz.Zbcode, 
       hz.PluCode, 
       hz.pluname, 
       hz.MATERIALCODE,
       hz.CARGONO,
       hz.postcode,
       hz.PLUMODEL,
       hz.clscode, 
       hz.clsname,  
       count(distinct hz.preorgcode) FgsCounts,
       count(distinct hz.ORGCODE) BraCounts,
       count(distinct hz.PFSaleNo) PFSaleCounts, 
       count(distinct hz.CUSTCODE) cutcounts,
       count(distinct hz.Rzdate) RzCounts,
       count(distinct hz.PluCode) PluCounts,
       sum(hz.Counts) Totalcounts,
       sum(hz.ystotal) Totalys,
       sum(hz.PFTotal) PFTotal
    from 
        (Select g.preorgcode,
               i.ORGCODE, 
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               i.CUSTCODE || i.ORGCODE CUSTCODE, 
               f.Rzdate || i.CUSTCODE || i.ORGCODE Rzdate,
               f.PFSaleNo,
               en.postcode,
               d.PluCode, 
               t.pluname, 
               t.MATERIALCODE,
               t.CARGONO,
               t.PLUMODEL,
               p.clscode, 
               p.clsname, 
               d.Counts,
               d.ystotal,
               d.PFTotal
          From tSkuplu                  t,
               tCatCategory              p, 
               torgdrpdisp               h,  
               tOrgDrpRelation           g, 
               tDrpEtpCustform           i, 
               tDrpEtpCustdetail         e,
               tmgpfsaleform_rpt@hsrpt   f,
               tmgpfsaledetail_rpt@hsrpt d,
               tEtpEnterprise            en
         Where f.PFSaleNo = d.pfsaleno and f.branchno = d.branchno 
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode 
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid  
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%完美有效%' 
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1' 
           and f.branchno = en.etpcode
           and f.Rzdate between '2017-09-01' and '2017-09-10'
        union all
        Select ''             as preorgcode, 
               ''             as ORGCODE, 
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               ''             as CUSTCODE, 
               ''             as Rzdate,   
               ''             as PFSaleNo,
               en.postcode,
               d.PluCode, 
               t.pluname, 
               t.MATERIALCODE,
               t.CARGONO,
               t.PLUMODEL,
               p.clscode, 
               p.clsname, 
               -d.Counts      as Counts,
               -d.yttotal     as ystotal,
               -d.PFTotal     as PFTotal
          From tSkuplu                    t,
               tCatCategory                p, 
               torgdrpdisp                 h, 
               tOrgDrpRelation             g, 
               tDrpEtpCustform             i, 
               tDrpEtpCustdetail           e,
               tmgpfreturnform_rpt@hsrpt   f,
               tmgpfreturndetail_rpt@hsrpt d,
               tEtpEnterprise              en
         Where f.PFreturnNo = d.pfreturnno and f.branchno = d.branchno
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode  
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid 
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%完美有效%'
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1'
           and f.branchno = en.etpcode
           and f.Rzdate between '2017-09-01' and '2017-09-10') hz
    Group By Zbcode, PluCode, pluname, postcode, clscode, clsname, MATERIALCODE, CARGONO, PLUMODEL
    )x
on a.Zbcode = x.Zbcode;


select count(m.orgcode), hz.postcode
from
       (Select distinct i.ORGCODE, 
               i.CUSTCODE
          From tDrpEtpCustform         i,
               tDrpEtpCustdetail       e
          where e.kid = i.kid      
          and e.ORGCODE in (select ORGCODE from torgdrpdisp where remark like '%' || '%' ||'%')       
          and e.STATUS like '%' || 0 || '%'
          and i.STATUS like '%' || 0 || '%'
          and e.ISPERFECT = '1'
          and e.isxuesheng = '1'
          ) m
  left join 
      (Select  f.ORGCODE,
               f.CUSTCODE, 
               en.etpcode,
               en.postcode
          From tDrpEtpCustdetail         f,  
               tOrgDrpRelation           g,
               tetpenterprise            en
         Where f.Orgcode = g.orgcode      
           and f.ISPERFECT = '1' 
           and f.isxuesheng = '1'
           and en.etpcode = f.Orgcode
) hz   
   on m.ORGCODE = hz.ORGCODE and m.CUSTCODE = hz.CUSTCODE 
   group by postcode
   ;