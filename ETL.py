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