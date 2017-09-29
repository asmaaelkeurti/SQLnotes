select 
	fgscode,
	fgsname,
	etplevel,
	etptype,
	plucode,
	pluname,
	materialcode,
	cargono,
	sum(pfcount) pfcount,
	sum(pftotal) pftotal,
	sum(ystotal) ystotal
from
(
  select        
                  
                  h.OrgCode fgscode, 
                  (select jskhname from tr_rpt_fgsjskh where jskhcode = h.ORGCODE) fgsname,
                 (select PostCode from tEtpEnterprise where Etpcode=(select EtpEName from tetpenterprise where etpcode=h.etpcode)) etplevel,
                 (select  Email from tEtpEnterprise where Etpcode=(select EtpEName from tetpenterprise where etpcode=h.etpcode)) etptype, 
                  c.plucode plucode,
                  c.pluname pluname,
                  c.materialcode materialcode,
                  c.cargono cargono,
                  b.pfcount  pfcount,
                  b.pfcount*b.PfPrice  pftotal,
                  b.StlCurrSsTotal ystotal     
from twslxshead h, twslxsBody b,tskuplu c,tcatcategory d, torgdrpdisp hh
where     h.billno = b.billno
      and c.clsid =  d.clsid
      and c.plucode = b.plucode
      and d.clscode like '0%'
      and h.billtype='0'
      --and h.OrgCode like '%'|| vs_fgscode || '%'
      --and h.etpcode like '%'|| vs_etpcode || '%'
      --and c.plucode like vs_plucode || '%' 
      --and d.clscode like vs_clscode || '%' 
      and h.jzdate is not null
      and to_char(h.jzdate,'yyyy-mm-dd') between  '2017-06-22' and '2017-09-22'
      and hh.orgcode = h.etpcode
      and hh.remark like '%有效%'    
union all
 select           
                  h.OrgCode fgscode, 
                  (select jskhname from tr_rpt_fgsjskh where jskhcode = h.ORGCODE) fgsname,
                 (select PostCode from tEtpEnterprise where Etpcode=(select EtpEName from tetpenterprise where etpcode=h.etpcode)) etplevel,
                 (select  Email from tEtpEnterprise where Etpcode=(select EtpEName from tetpenterprise where etpcode=h.etpcode)) etptype, 
                  c.plucode plucode,
                  c.pluname pluname,
                  c.materialcode materialcode,
                  c.cargono cargono,
                  -b.pfcount  pfcount,
                  -b.pfcount*b.PfPrice  pftotal,
                  -b.StlCurrSsTotal ystotal          
from twslxshead h, twslxsBody b,tskuplu c,tcatcategory d,torgdrpdisp hh
where     h.billno = b.billno
      and c.clsid =  d.clsid
      and c.plucode = b.plucode
      and d.clscode like '0%'
      and h.billtype='1'
      --and h.OrgCode like '%'|| vs_fgscode || '%' 
      --and h.etpcode like '%'|| vs_etpcode || '%'
      --and c.plucode like vs_plucode || '%' 
      --and d.clscode like vs_clscode || '%' 
      and h.jzdate is not null
      and to_char(h.jzdate,'yyyy-mm-dd') between  '2017-06-22' and '2017-09-22'
      and hh.orgcode = h.etpcode
      and hh.remark like '%有效%'
)
group by fgscode,fgsname,etplevel,etptype,plucode,pluname,materialcode,cargono;