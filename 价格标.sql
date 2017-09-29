select 
        currcode,
        b.plucode,
        (SELECT PLUNAME FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) PLUNAME,
        (SELECT UNIT FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) UNIT,
        (SELECT SPEC FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) SPEC,
        (SELECT CARGONO FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) CARGONO,
        (SELECT MATERIALCODE FROM TSKUPLU WHERE PLUCODE = B.PLUCODE) MATERIALCODE,
        max(b.price) as price
from tPrcPsJgzcHead h, tPrcPsJgzcBody b
where h.jgzccode = b.jgzccode
  and currcode = '0' 
  and length(h.orgcode) = 1 group by currcode, b.plucode
  ;