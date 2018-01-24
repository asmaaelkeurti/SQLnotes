# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 15:45:55 2017

@author: 150972
"""
#import pandas as pd
#asmaa = pd.read_csv('c:\\Users\\150972\\Desktop\\working\\sales-asmaa.csv', encoding='gbk')
#d = asmaa.columns[11:]
#cols = ['料号','型号','品名','value','date']
#irene = pd.DataFrame(columns=cols)
#
#c = ['料号','型号','品名']
#
#for i in d:
#    print(i)
#    c.append(i)
#    temp = asmaa[c]
#    temp[i].fillna(0,inplace=True)
#    #temp = temp[temp[i].notnull()]
#    temp['date'] = i
#    temp = temp.rename(columns={i:'value'})
#    irene = pd.concat([irene,temp])
#    c.pop()
#    
#irene.to_csv('c:\\Users\\150972\\Desktop\\working\\irene.csv')

import cx_Oracle as oracle
import pandas as pd

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
month = [   
            ['2017-12-01','2017-12-31'],
            ['2017-11-01','2017-11-30'],
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
            
#            ['2015-12-01','2015-12-31'],
#            ['2015-11-01','2015-11-30'],
#            ['2015-10-01','2015-10-31'],
#            ['2015-09-01','2015-09-30'],
#            ['2015-08-01','2015-08-31'],
#            ['2015-07-01','2015-07-31'],
#            ['2015-06-01','2015-06-30'],
#            ['2015-05-01','2015-05-31'],
#            ['2015-04-01','2015-04-30'],
#            ['2015-03-01','2015-03-31'],
#            ['2015-02-01','2015-02-29'],
#            ['2015-01-01','2015-01-31'],
        ]


skuQuery = """select p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,
    Lane2018,Lane2017,
    (case when f.evaluationname IS NOT NULL THEN f.evaluationname
          when e.clsname IS NOT NULL THEN '考试项目'
          when p.functionality = 'C' THEN '考试项目'
          when p.highlevel = '物料' THEN '物料'
          else '其它品类'
    END) adjusted
    from
(select p.pluid, p.plucode, p.pluModel, p.MaterialCode, p.pluname, p.LRDate, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp9 functionality, p.udp14 flag,
        n.Lane2018,n.Lane2017
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p, notSigned2018@finance n
where substr(a.clscode,1,1) = b.clscode 
		and substr(a.clscode,1,3) = c.clscode 
		and substr(a.clscode,1,5) = d.clscode 
		and p.clsid = a.clsid
		and substr(a.clscode,1,1) = 0
        and p.MaterialCode = n.MaterialCode
        and p.PLUCODE = n.PLUCODE
        ) p 
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
        group by p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,
                Lane2018,Lane2017"""
                
skuQuery = """select p.pluid,p.plucode,p.cargono,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,sd.sdtotal,IPO.listDate,
    n.Lane2017,
    (case when f.evaluationname IS NOT NULL THEN f.evaluationname
          when e.clsname IS NOT NULL THEN '考试项目'
          when p.functionality = 'C' THEN '考试项目'
          when p.highlevel = '物料' THEN '物料'
          else '其它品类'
    END) adjusted
    from
(select p.pluid, p.plucode, p.cargono,p.pluModel, p.MaterialCode, p.pluname, p.LRDate, p.clsid, a.clscode, b.clsname product,c.clsname highLevel,d.clsname midLevel,a.clsname lowLevel, p.udp9 functionality, p.udp14 flag
  from tcatcategory a, tcatcategory b, tcatcategory c, tcatcategory d,  tskuplu p
where substr(a.clscode,1,1) = b.clscode 
    and substr(a.clscode,1,3) = c.clscode 
    and substr(a.clscode,1,5) = d.clscode 
    and p.clsid = a.clsid
    and substr(a.clscode,1,1) = 0
        ) p 
        left join notSigned2018@finance n
            on p.pluid = n.pluid
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
                and h.orgcode in ('1', '2','5') --h.depid in ('10010000000021','10010000000022')  
                and h.depid <> '10010000000023' --部门
            group by b.materialcode
        )IPO on IPO.materialcode = p.materialcode
        group by p.pluid,p.plucode,p.cargono,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,sd.sdtotal,IPO.listDate,
                n.Lane2017
"""

skuData = pd.read_sql(skuQuery,con=con)


salesQuery = """select u.materialcode,sum(u.pstotal) total
  From (
  select b.plucode,b.pluname,b.materialcode,b.pstotal*1000 pstotal
  from tdstpshead h, tdstpsbody b
  where h.billno = b.billno
      and h.orgcode in ('1', '2','5') --h.depid in ('10010000000021','10010000000022')  
      and h.depid <> '10010000000023' --部门
      and to_char(h.jzdate, 'YYYY-MM-DD') between '%s' and '%s' 
  union all
  select b.plucode, b.pluname, b.materialcode, -b.thtotal*1000 pstotal
  from tdstrtnhead h,tdstrtnbody b
  where h.billno = b.billno
      and h.orgcode in ('1', '2','5') --h.depid in ('10010000000021','10010000000022')  
      and h.depid <> '10010000000023' --部门
      and to_char(h.jzdate, 'YYYY-MM-DD') between '%s' and '%s') u 
  group by u.materialcode"""


for m in month:
    print(m)
    data = pd.read_sql(salesQuery % (m[0],m[1],m[0],m[1]),con=con)
    data['TOTAL'] = data['TOTAL']/1000.0
    data = data.rename(columns={'TOTAL':m[0]})
    skuData = pd.merge(skuData,data,how='left',on=['MATERIALCODE'])
    skuData.loc[skuData['CLSCODE']=='0100101',m[0]] = skuData.loc[skuData['CLSCODE']=='0100101',m[0]]/2.0

skuData.to_csv('c:\\Users\\150972\\Desktop\\fanxu-horizontal.csv')

#skuData = pd.read_sql(skuQuery,con=con)
#del skuData['PRICE']
#
#cols = skuData.columns
#cols = list(cols)
#cols.append('TOTAL')
#fanxu = pd.DataFrame(columns=cols)
#
#for m in month:
#    print(m)
#    data = pd.read_sql(salesQuery % (m[0],m[1],m[0],m[1]),con=con)
#    data['TOTAL'] = data['TOTAL']/1000.0
#    temp = pd.merge(skuData,data,how='left', on=['MATERIALCODE'])
#    temp['date'] = m[0]
#    temp['TOTAL'].fillna(0,inplace=True)
#    temp.loc[temp['CLSCODE']=='0100101','TOTAL'] = temp.loc[temp['CLSCODE']=='0100101','TOTAL']/2.0
#    fanxu = pd.concat([fanxu,temp])
#
#    
#
#fanxu.to_excel('c:\\Users\\150972\\Desktop\\fanxu-vertical.xlsx',index=False)

