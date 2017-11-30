# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 14:38:06 2017

@author: 150972
"""
import pandas as pd
JinPinList = pd.read_excel('c:\\Users\\150972\\Desktop\\working\\JinPinList.xlsx', encoding='gbk')
product = pd.read_csv('c:\\Users\\150972\\Desktop\\working\\product.csv', encoding='gbk') #gb18030

product['PLUCODE'] = product['PLUCODE'].apply(lambda x: '{0:0>8}'.format(x))

JinPinList = JinPinList.rename(columns={'料号':'MATERIALCODE','商品编码':'PLUCODE'})

newJinPin = JinPinList[['MATERIALCODE','PLUCODE']]

newJinPin['PLUCODE'] = newJinPin['PLUCODE'].apply(lambda x: '{0:0>8}'.format(x))

#asmaa = newJinPin.to_frame()
newJinPin['JinPing'] = 1

result = pd.merge(product,newJinPin, how='left',on=['PLUCODE','MATERIALCODE'])

#result[result['type'] == '精品清单']

result['BenWei'] = 0
result['BenWei'] = np.where(result['PLUNAME'].str.contains("本味"),1,0)
result['Meetape'] = np.where(result['PLUNAME'].str.contains("Meetape"),1,0)
#result[ids.isin(ids[ids.duplicated()])].sort('MATERIALCODE')

#result[result['PLUNAME'].str.contains("本味")]

#test['BenWei'] = np.where(test['PLUCODE'].str.contains("本味"),1,0)

import numpy as np

def openFile(DC, file):
    newFile = pd.read_csv('c:\\Users\\150972\\Desktop\\working\\' + file,encoding='gb18030')
    newFile = newFile.drop(newFile.index[len(newFile)-1])
    newDC = pd.concat([DC,newFile])
    return newDC
    
pd.concat([DC,DC2])
asmaa = asmaa.drop(asmaa.index[len(asmaa)-1])

begin['CUSTCODE'] = begin['CUSTCODE'].apply(lambda x: '{0:0>8}'.format(x))

join = pd.merge(begin,tail,on='CUSTCODE')
len(join)
join = pd.merge(begin,right,how='left',on=['CUSTCODE','KID'])

beginCust = begin['CUSTCODE']

uniqueBeginCust = beginCust.unique()
uniqueBeginCust
len(uniqueBeginCust)
uniqueTailCust = tailCust.unique()
len(uniqueTailCust)
import numpy as np
join = np.append(uniqueBeginCust,uniqueTailCust)
joinSeries = pd.Series(join)
asmaa['CUSTCODE'].isin(constantCust)
joinSeries = pd.Series(join)
len(joinSeries)
nonRepeated = joinSeries.duplicated()
len(nonRepeated)
nonRepeated
asmaa['CUSTCODE'].isin(nonRepeated)
asmaa[asmaa['CUSTCODE'].isin(nonRepeated)]
len(asmaa[asmaa['CUSTCODE'].isin(nonRepeated)])
len(asmaa)
nonRepeated
len(nonRepeated)
len(begin)
len(uniqueBeginCust)
len(uniqueTailCust)
len(asmaa[asmaa['CUSTCODE'].isin(nonRepeated)])
len(asmaa)
asmaa['ConstantStore'] = np.where(asmaa['CUSTCODE'].isin(nonRepeated),1,0)
asmaa
asmaa.to_csv('c:\\Users\\150972\\Desktop\\working\\flagAdded.csv')
asmaa
asmaa['orgcode']
asmaa['ORGCODE']
asmaa['uniqueCust'] = asmaa['ORGCODE'] + asmaa['CUSTCODE']
asmaa['CUSTCODE'] = asmaa['CUSTCODE'].astype(str)
asmaa['CUSTCODE']
asmaa['CUSTCODE'] = asmaa['CUSTCODE'].apply(lambda x: '{0:0>8}'.format(x))
asmaa['CUSTCODE']
asmaa['uniqueCust'] = asmaa['ORGCODE'] + asmaa['CUSTCODE'].map(str)
asmaa['uniqueCust']
begin = asmaa[(asmaa['date'] >= '2016-01-01') & (asmaa['date'] <= '2016-04-01')]['uniqueCust'] 
begin
len(begin)
beginCust = begin.unique()
len(beginCust)
tail = asmaa[(asmaa['date'] >= '2017-07-01') & (asmaa['date'] <= '2017-09-30')]['uniqueCust']

df = dd.read_csv('c:\\Users\\150972\\Desktop\\data.csv',encoding='gbk')
df['PLUCODE'] = df['PLUCODE'].apply(lambda x: '{0:0>8}'.format(x))
df = df.merge(skuData,how='left',on=['PLUCODE'])
result = df[df['TIMERANGE']=='2016-01-01'].compute()
result
result = df[df['TIMERANGE']=='2016-01-01'].groupby('FLAG').sum().compute()
result
result = df[df['TIMERANGE']<'2016-02-01'].groupby('FLAG').sum().compute()
result
result = df[df['TIMERANGE']<'2016-02-01']['YSTOTAL'].groupby('FLAG').sum().compute()
result = df[df['TIMERANGE']<'2016-02-01'][['YSTOTAL','FLAG']].groupby('FLAG').sum().compute()
result
result = df[['PFTOTAL','FLAG']].groupby('FLAG').sum().compute()
result
result = df[['PFTOTAL','FLAG','TIMERANGE']].groupby(['FLAG','TIMERANGE']).sum().compute()
result
result = df[['PFTOTAL','FLAG','TIMERANGE']].groupby(pd.TimeGrouper(freq='M')).sum().compute()
result = df[['PFTOTAL','FLAG','TIMERANGE']].groupby([df['TIMERANGE'].month]).sum().compute()
result = df[['PFTOTAL','FLAG','TIMERANGE']].groupby([df['TIMERANGE'][:7]]).sum().compute()
skuData.columns

tailCust = tail.unique()
len(tailCust)
join = np.append(beginCust,tailCust)
len(join)
type(join)
joinSeries = pd.Series(join)
uniqueJoin = joinSeries.unique()
len(uniqueJoin)
nonRepeated = joinSeries[joinSeries.duplicated()]
len(nonRepeated)
nonRepeated
asmaa['ConstantStore'] = np.where(asmaa['CUSTCODE'].isin(nonRepeated),1,0)
asmaa['ConstantStore'].sum()
asmaa['ConstantStore'] = np.where(asmaa['uniqueCust'].isin(nonRepeated),1,0)
asmaa['ConstantStore'].sum()
asmaa.to_csv('c:\\Users\\150972\\Desktop\\working\\flagAdded.csv')

asmaa.replace('\n','',regex=True,inplace=True)

import cx_Oracle as oracle
con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
cur.execute('select * from tskuplu where rownum < 10')
query = """select u.plucode, u.pluname, u.materialcode,sum(u.pstotal) total
  From (
  select b.plucode,b.pluname,b.materialcode,b.pstotal*1000 pstotal
  from tdstpshead h, tdstpsbody b
  where h.billno = b.billno
      and h.orgcode in ('1', '2') --h.depid in ('10010000000021','10010000000022')  
      and h.depid <> '10010000000023' --部门
      and to_char(h.jzdate, 'YYYY-MM-DD') between '2017-09-01' and '2017-09-31'
union all
  select b.plucode, b.pluname, b.materialcode, -b.thtotal*1000 pstotal
  from tdstrtnhead h,tdstrtnbody b
  where h.billno = b.billno
      and h.orgcode in ('1', '2') --h.depid in ('10010000000021','10010000000022')  
      and h.depid <> '10010000000023' --部门
      and to_char(h.jzdate, 'YYYY-MM-DD') between '2017-09-01' and '2017-09-31') u 
  group by u.plucode, u.pluname, u.materialcode"""
asmaa = pd.read_sql('select * from tskuplu',con=con)
asmaa = pd.read_sql(query,con=con)




import cx_Oracle as oracle
import pandas as pd

skuQuery = """select p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,
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
        group by p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price"""

skuData = pd.read_sql(skuQuery,con=con)

con = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')
skuQuery = """select p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price,
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
        group by p.pluid,p.plucode,p.pluModel,p.materialcode,p.pluname,p.LRDate,p.clsid,p.clscode,p.product, p.highlevel, p.midlevel, p.lowlevel, p.functionality, p.flag, e.clsname, f.evaluationname,price.price"""

skuData = pd.read_sql(skuQuery,con=con)

df = dd.read_csv('c:\\Users\\150972\\Desktop\\data.csv',encoding='gbk')
import dask.dataframe as dd
df = dd.read_csv('c:\\Users\\150972\\Desktop\\data.csv',encoding='gbk')
df['PLUCODE'] = df['PLUCODE'].apply(lambda x: '{0:0>8}'.format(x))
df = df.merge(skuData,how='left',on=['PLUCODE'])
len(df[df['TIMERANGE']=='2016-01-01'])

irene = data[['KID','LANE2017','COSTOFGOODSSOLD','RZCOUNTS','PFCOUNTS','date']]
result = irene.groupby(['KID','date','LANE2017']).sum()
result = result.reset_index()

data = pd.read_csv('F:\\DataWarehouse\\10-20.csv',encoding='gbk')
irene = data[['KID','LANE2017','COSTOFGOODSSOLD','RZCOUNTS','PFCOUNTS','date']]
result = irene.groupby(['KID','date','LANE2017']).sum()
result.columns
result = result.reset_index()
result.columns
len(result)    
begin = result[(result['date'] >='2016-01-01') & (result['date'] <= '2016-03-30')]['kid']
begin = result[(result['date'] >='2016-01-01') & (result['date'] <= '2016-03-30')]['KID']
begin
len(begin)
len(irene)
result[['KID']==55672]
result[result['KID']==55672]
result[result['KID']=='55672']
result[result['KID']==55672]
result['KID']
result[result['KID']==55672]
data
data[data['KID'] == 55672]
data['KID']
len(data['KID'].unique())
begin = result[(result['date'] >='2016-01-01') & (result['date'] <= '2016-03-30')]['KID']
len(begin)
type(begin)
len(begin.unique())
begin
uniqueBeginCust = beginCust.unique()
uniqueBeginCust = begin.unique()
tail = result[(result['date'] >='2017-07-01') & (result['date'] <= '2017-09-30')]['KID']
len(tail)
len(uniqueBeginCust)
len(tail.unique())
uniqueTailCust = tail.unique()
len(uniqueTailCust)
join = np.append(uniqueBeginCust,uniqueTailCust)
import numpy as np
join = np.append(uniqueBeginCust,uniqueTailCust)
type(join)
len(join)
join.unique()
join = pd.Series(join)
len(join.unique())
len(join.duplicated())
len(join)
len(uniqueBeginCust)
len(uniqueTailCust)
len(join)
5445 + 6989
repeated = join.duplicated()
len(repeated)
join
join.duplicated()
join[join.duplicated()]
len(join[join.duplicated()])
comparable = join[join.duplicated()]
len(comparable)
len(uniqueBeginCust)
len(uniqueTailCust)
len(asmaa[asmaa['CUSTCODE'].isin(comparable)])
len(irene[irene['CUSTCODE'].isin(comparable)])
len(irene[irene['KID'].isin(comparable)])
len(irene)
len(result)
len(result[result['KID'].isin(comparable)])
result['Comparable'] = np.where(result['KID'].isin(comparable),1,0)
result.columns
result
result.to_csv('c:\\Users\\150972\\Desktop\\working\\irene.csv')
result
len(data)
data.columns
channels = data[['KID','ISBANGONG','ISJINGPIN','ISXUESHENG']].groupby('KID').first()
channels
len(channels)
newResult = pd.merge(result,channels,how='left',on=['KID'])
result.columns
channels
channels = channels.reset_index()
newResult = pd.merge(result,channels,how='left',on=['KID'])
len(newResult)
newResult.to_csv('c:\\Users\\150972\\Desktop\\working\\irene.csv')
len(data)
data.columns
data['COSTOFGOODSSOLD'].sum()
data['RZCOUNTS'].sum()
data['PFCOUNTS'].sum()
comparable
cassandra = pd.read_csv('F:\\DataWarehouse\\10-20-sum.csv',encoding='gbk')
len(cassandra)
cassandra.columns
carina = cassandra[['KID','date','LANE2017','RZCOUNTSTOTAL']]
carina = cassandra[['KID','date','RZCOUNTSTOTAL']]
carina
carina = carina.groupby(['KID','date']).sum()
len(carina)
carina['Comparable'] = np.where(carina['KID'].isin(comparable),1,0)
carina = carina.reset_index()
carina['Comparable'] = np.where(carina['KID'].isin(comparable),1,0)
len(carina[carina['Comparable']==1])
len(channels)
newChannels = cassandra[['KID','ISBANGONG','ISJINGPIN','ISXUESHENG']].groupby('KID').first()
cassandra.columns
newResult1 = pd.merge(carina,channels,how='left',on=['KID'])
len(newResult1)
newResult1.to_csv('c:\\Users\\150972\\Desktop\\working\\carina.csv')

asmaa = data[data['comparable'] == 1][['KID','date','Lane','channel']]
len(asmaa)
len(result)
for index, row in result.iterrows():
    print('%s %s %s ' % (row['date'] row['Lane'],row['channel']))
for index, row in result.iterrows():
    print('%s %s %s ' % (row['date'], row['Lane'],row['channel']))
    
asmaa[(asmaa['Lane'] == '大众') & (asmaa['channel'] == '学生渠道') & (asmaa['date'] <= '2017-01-01')]['KID'].nunique() 
asmaa[(asmaa['Lane'] == '大众') & (asmaa['channel'] == '学生渠道') & (asmaa['date'] == '2017-01-01')]['KID'].nunique()
for index, row in result.iterrows():
    row['KID'] = asmaa[(asmaa['Lane'] == row['Lane']) & (asmaa['channel'] == row['channel']) & (asmaa['date'] <= row['date'])]['KID'].nunique()
    
result
result.to_csv('c:\\Users\\150972\\Desktop\\working\\当月累计有进货店数.csv')

result = asmaa.groupby(['date','Lane','channel']).KID.nunique()
result
result = result.reset_index()
result

for index, row in result.iterrows():
    result.set_value(index,'KID', data[(data['CHANNEL'] == row['CHANNEL']) & (data['date'] <= row['date'])]['KID'].nunique())