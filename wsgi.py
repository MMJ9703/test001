# 添加所需要的库
import os
# 添加自己编写的算法
from Import_data import mainf
# from read_data import read_xml, read_http
# Web应用程序设置
from flask_executor import Executor
import pandas as pd
from threading import Thread

from sqlalchemy import create_engine


from flask import Flask
# from flask import request
# from flask_cors import *

application = Flask(__name__)
executor = Executor(application)
# 获取mysql环境变量
# os.environ['MYSQL_URI'] = 'mysql+pymysql://root:123456@localhost:3306/test'
env = os.environ
# MYSQL_URI   mysql+pymysql://test:test@172.30.238.185:3306/test
mysql_uri = env.get('MYSQL_URI')

sqlEngine = create_engine(mysql_uri, pool_recycle=3600)

print ('=== mysql uri: ' + mysql_uri)

def threaded_task():  
    try:

        print ('===== run task')

        # 机组信息
        ID = 150817080435211
        Input_Time = '2020-6-20 00:00:00'
        days = 1
        # ID,Input_Time,days = read_xml()
        # data = request.get_json()
        # ID,Input_Time,days = read_http(data)
        # 算法执行
        status = mainf(ID, Input_Time, days, sqlEngine)
        # data["param"]["status"] = status
        print(status)
        # 将数据写入mysql数据库中，设置查询pd.to_sql()函数
        
        

    except Exception as e:
        print ('===error===')
        print (e)
        raise e
    return True

# rest  api（应用执行端口）
@application.route('/')
def hello():
    executor.submit(threaded_task)
    return b'mainf '

if __name__ == '__main__':

    application.run()

