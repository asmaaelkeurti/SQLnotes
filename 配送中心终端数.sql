select count(m.orgcode), hz.postcode
from
       (Select distinct i.ORGCODE, 
               i.CUSTCODE
          From tDrpEtpCustform         i,
               tDrpEtpCustdetail       e
          where e.kid = i.kid      
          and e.ORGCODE in (select ORGCODE from torgdrpdisp where remark like '%' || '完美有效' ||'%')       
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
   group by postcode;