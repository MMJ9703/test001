import pandas as pd

data=pd.read_csv('C:/Users/pc6/Desktop/寄云/1.csv',encoding='gbk')

x = data['x']
y = data['y']


def fun(data):
    x = data['x']
    y = data['y']
    z = x + y
    return z

z = fun(data)