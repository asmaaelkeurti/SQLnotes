select plucode, materialcode, etplevel,qudao,sum(cbtotal) cbtotal
from(
  select        
                  h.OrgCode fgscode, 
                  h.etpcode etpcode,
                  c.plucode plucode,
				  c.materialcode materialcode,
                  Nvl((Select udp14 From tskuplu Where plucode =c.plucode ),'大众') As udp14,
                  f.etplevel As etplevel,
                  f.qudao As qudao,
                 (Case h.billtype When '0' Then b.pfcount Else -b.pfcount End)*e.lsavghjprice/10000 As cbtotal
from twslxshead h, twslxsBody b,tskuplu c,tcatcategory d,tskuetpparas e,tetptemp f
where     h.billno = b.billno
      and c.clsid =  d.clsid
      and c.plucode = b.plucode
      and h.orgcode not in('054')
      and d.clscode like '0%'
      and d.clscode not like '011%'
      and h.jzdate is not null
      and f.etpcode=h.etpcode
      and f.fgscode=h.orgcode
      and f.etplevel in('其它')
      And h.orgcode = e.orgcode And c.pluid = e.pluid and e.ismainetp = '1'
      and to_char(h.jzdate,'yyyy-mm-dd') between '2017-01-01' and '2017-09-20')
group by plucode, materialcode ,etplevel,qudao;