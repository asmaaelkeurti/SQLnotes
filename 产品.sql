spool "E:\load from oracle\²úÆ·.csv"

select /*csv*/ p.pluid, p.plucode, p.MaterialCode,p.pluname, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp14 flag   
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
		and substr(a.clscode,1,3) <> 011;
        
spool off