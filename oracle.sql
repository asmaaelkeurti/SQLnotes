select d.orgcode, d.custcode, d.custname, d.address, e.etpname, e.etpcode, e.cetpname, e.postcode,e.email
from TDRPETPCUSTDETAIL d,TETPENTERPRISE e
where d.Orgcode = e.etpcode and d.isperfect = '1' and d.isxuesheng = '1';

select * from tetpenterprise;

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
where f.rzdate = '2017-09-19' 
  and f.pfsaleno = d.pfsaleno 
  and f.branchno = d.branchno
  and d.plucode = plu.plucode
  and f.customercode = c.custcode
  and f.BranchNo = e.etpcode;
  
  

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
  Group by Zbcode;
  
  select count(distinct(highLevel)), count(highLevel) from 
  (select p.pluid, p.plucode, p.MaterialCode,p.pluname, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp14 flag   
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
		and substr(a.clscode,1,3) <> 011) group by highLevel, product, midLevel;
        
Select          g.preorgcode,
               i.ORGCODE, 
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               i.CUSTCODE || i.ORGCODE CUSTCODE, 
               f.Rzdate || i.CUSTCODE || i.ORGCODE Rzdate,
               f.PFSaleNo,
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
               tmgpfsaledetail_rpt@hsrpt d
         Where f.PFSaleNo = d.pfsaleno and f.branchno = d.branchno 
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode 
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid  
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%有效%' 
           and d.plucode = 00000388
           --and (d.plucode like  '%'||vs_plucode||'%' or t.materialcode like  '%'||vs_plucode||'%')
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1' 
           and f.Rzdate between '2017-09-01' and '2017-09-01'
        union all
        Select g.preorgcode,
               i.ORGCODE, 
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               i.CUSTCODE || i.ORGCODE CUSTCODE, 
               f.Rzdate || i.CUSTCODE || i.ORGCODE Rzdate,
               f.PFSaleNo,
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
               tmgpfreturndetail_rpt@hsrpt d
         Where f.PFreturnNo = d.pfreturnno and f.branchno = d.branchno
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode  
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid 
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%有效%'
           and d.plucode = 00000388
           --and (d.plucode like  '%'||vs_plucode||'%' or t.materialcode like  '%'||vs_plucode||'%') 
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1'
           and f.Rzdate between '2017-09-01' and '2017-09-01';
           
select * from tetpenterprise where etpcode = '0852';

select count(isbangong) from TDRPETPCUSTDETAIL;

select count(*) 
from tdrpetpcustdetail c, torgdrprelation r
where c.orgcode = r.orgcode;

select p.pluid, p.plucode, p.MaterialCode,p.pluname, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp14 flag   
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
		and substr(a.clscode,1,3) <> 011;
       
       
select 
        currcode,
        b.plucode,
        b.pluid,
        (SELECT PLUNAME FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) PLUNAME,
        (SELECT UNIT FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) UNIT,
        (SELECT SPEC FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) SPEC,
        (SELECT CARGONO FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) CARGONO,
        (SELECT MATERIALCODE FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) MATERIALCODE,
        --(select min(lsavghjprice) from tskuetpparas where pluid = b.pluid) pricels,
        max(b.price) as price        
from tPrcPsJgzcHead h, tPrcPsJgzcBody b
where h.jgzccode = b.jgzccode
  and currcode = '0' 
  and length(h.orgcode) = 1 group by currcode, b.plucode, b.pluid
  ;
  
  select * from tskuetpparas where pluid = '800001700';