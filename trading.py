import pandas as pd
import quandl
import matplotlib.pyplot as plt
import numpy as np

# gold data
#asmaa = quandl.get("LBMA/GOLD", uthtoken="MU4755ALSYYj1suiJgsd",start_date="2000-01-01")[['USD (AM)']].rename(columns={'USD (AM)':'price'})

#a = a.rename(columns={'USD (AM)':'price'})

# spy data
#cassan = pd.read_csv('C:\\Users\\150972\\Desktop\\working\\HistoricalQuotes.csv')

class account:
    def __init__(self,init):
        self.accountbalance = init
        self.holdingShares = 0
        self.buyingpower = init
        self.lastTradePrice = 0
    
    def buy(self,shares,price):
        self.holdingShares = self.holdingShares + shares
        self.buyingpower = self.buyingpower - shares*price
        self.update(price)
    
    def sell(self,shares,price):
        self.holdingShares = self.holdingShares - shares
        self.buyingpower = self.buyingpower + shares*price
        self.update(price)
    
    def update(self,price):
        self.accountbalance = self.buyingpower + self.holdingShares*price
        self.lastTradePrice = price
    
    def __str__(self):
        return 'accountbalance:%s holdingShares:%s buyingPower:%s lastTradePrice:%s' % (self.accountbalance,self.holdingShares,self.buyingpower,self.lastTradePrice)
    
    
    

class trade:
    def __init__(self,data):
        self.data = data
        self.history = []
        
    def strategy_1(self,p1):
        a = account(10000)
        
        for index,row in self.data.iterrows():
            hist = self.data.loc[:index].tail(p1)
            cPrice = row['price']
            if cPrice >= hist['price'].max():
                if a.holdingShares == 0:
                    a.buy(int(a.buyingpower*0.8/cPrice),cPrice)
                    self.log(a,index)

            elif cPrice <= hist['price'].min():
                if a.holdingShares != 0:
                    a.sell(a.holdingShares,cPrice)
                    self.log(a,index)
                    
                    
    def log(self,account,index):
        self.history.append([account.accountbalance,account.holdingShares,account.buyingpower,account.lastTradePrice,index])
        
    def historyDF(self):
        return pd.DataFrame(self.history,columns=['balance','holdingshares','buyingpower','price','index'])
    
    def performance(self):
        h = self.historyDF()
        print('Account Balance: %s' % h.iloc[-1]['balance'])
        print('Trading TImes: %s' % len(h))
        print('trading APR: %s' % (h.iloc[-1]['balance']/10000.0))
        print('gold APR: %s' % (h.iloc[-1]['price']/h.iloc[1]['price']))
        
        
    def draw(self):
        h = self.historyDF()
        ax = plt.axes()
        
        ax.plot(h['index'],h['balance'])
        
        
        
        
    