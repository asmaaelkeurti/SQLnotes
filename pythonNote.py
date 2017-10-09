# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 16:51:53 2017

@author: 150972
"""
import pandas as pd

asmaa = pd.read_csv('E:\\load from oracle\\4x4.csv', encoding = "gb2312")
asmaa['channel'] = asmaa['LANE']

for index, row in asmaa.iterrows():
    if row['ISBANGONG'] == '1':
        row['channel'] = '办公'
    elif row['ISJINGPIN'] == '1' and row['ISBANGONG'] == '0':
        row['channel'] = '精品'
    else:
        row['channel'] = '学生'
		
		
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 14:38:06 2017

@author: 150972
"""
import pandas as pd
JinPinList = pd.read_excel('c:\\Users\\150972\\Desktop\\working\\JinPinList.xlsx', encoding='gbk')
product = pd.read_csv('c:\\Users\\150972\\Desktop\\working\\product.csv', encoding='gbk')

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

    


