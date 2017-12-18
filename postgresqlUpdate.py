import psycopg2
import pandas as pd
from datetime import date
import time
import unittest
import cx_Oracle as oracle
import dask.dataframe as dd


orcleCon = oracle.connect('mganalyze/mganalyze@192.168.0.118/DRPMID')

hqCheckQuery = """
    Select round(sum(pstotal)*1000,0)
  From (
Select h.shorgcode As orgcode,b.plucode,b.pstotal,b.pscount
  From tdstpsbody b,tdstpshead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '%s' And '%s'
   And h.orgcode In('1','2','5')
   and h.depid <> '10010000000023'
Union All
Select h.thorgcode As orgcode,b.plucode,-b.thtotal As pstotal, -b.thcount As pscount
  From tdstrtnbody b,tdstrtnhead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '%s' And '%s'
   And h.orgcode In('1','2','5')
   and h.depid <> '10010000000023'
       ) s,tskuplu p
 Where s.plucode = p.plucode
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%%' And clscode Not Like '011%%')"""
   
   
   

   
def checkHQdata(startDate,endDate):
    drpmid = pd.read_sql(hqCheckQuery % (startDate,endDate,startDate,endDate),con=orcleCon).iloc[0,0]/1000.0
    print('drpmid result: %s' % drpmid)
    data = dd.read_csv('F:\\Data\\HQFact\\HQ20*.csv',encoding='gbk',dtype={'ORGCODE':object,'PLUID':object})
    print('csv file result: %s' % data[(data['date'] >= startDate) & (data['date'] <= endDate)].PSTOTAL.sum().compute())
    

def checkBRdata(startDate,endDate):
    drpmid = pd.read_sql()


if __name__ == '__main__':
    checkHQdata('2017-01-01','2017-06-01')


class TestStringMethods(unittest.TestCase):
    
    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)


pgContext = "dbname='postgres' user='postgres' host='localhost' password='lwj380279011'"

def endDetailPGinsert(d):
    dd = d.strftime('%Y-%m-%d')
    data = pd.read_csv('F:\\Data\\FactWithCost\\%s.csv' % dd,encoding='gbk',dtype={'CUSTCODE':object,'PLUCODE':object})
    data = data.fillna(0)
    pgconn = psycopg2.connect(pgContext)
    cur = pgconn.cursor()
    
    copy_sql = """COPY e FROM stdin WITH CSV HEADER"""
    
    with open('F:\\Data\\FactWithCost\\%s.csv' % dd, 'r') as f:
        cur.copy_expert(sql=copy_sql,file=f)
        pgconn.commit()
        cur.close()
    #pgquery = 'insert into enddetail values (%s,%s,%s,%s,%s,%s,%s)'
    #cur.executemany(pgquery,data.values.tolist())
    #pgconn.commit()
    pgconn.close()
    
    return dd


def endDetailETL(startDate):
    pgconn = psycopg2.connect(pgContext)
    cur = pgconn.cursor()
    
    cur.execute("""delete from e where rzdate >= '%s'""" % startDate.strftime('%Y-%m-%d'))
    pgconn.commit()
    cur.close()
    pgconn.close()
    print('clean done')
    
    for i in pd.date_range(startDate,date(int(time.strftime('%Y')),int(time.strftime('%m')),int(time.strftime('%d')))):
        while True:
            try:
                i = print(endDetailPGinsert(i))
                break
            except:
                time.sleep(10)
                print('retry')


def insert(df,q,table_name):
    runQuery(q)
    df.to_csv('F:\\Data\\postgresqlTemp.csv',index=False)
    
    pgconn = psycopg2.connect(pgContext)
    cur = pgconn.cursor()
    copy_sql = """COPY %s FROM stdin WITH CSV HEADER""" % table_name
    with open('F:\\Data\\postgresqlTemp.csv', 'r') as f:
        cur.copy_expert(sql=copy_sql,file=f)
        pgconn.commit()
        cur.close()
    pgconn.close()
    print('success')

def runQuery(qList):
    for i in qList:
        pgconn = psycopg2.connect(pgContext)
        cur = pgconn.cursor()
        cur.execute(i)
        pgconn.commit()
        cur.close()
        pgconn.close()
        print(i)
    


q = ["""DROP TABLE test """,
     """CREATE TABLE test (PLUID varchar, PLUCODE varchar, MATERIALCODE varchar, 
                                   CLSCODE varchar)"""
    ]



