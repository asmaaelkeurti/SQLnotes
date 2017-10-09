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