
import os

from t_function import fun

from flask_executor import Executor
import pandas as pd
from threading import Thread

from sqlalchemy import create_engine


from flask import Flask
application = Flask(__name__)
executor = Executor(application)

env = os.environ
# MYSQL_URI   mysql+pymysql://test:test@172.30.238.185:3306/test
mysql_uri = env.get('MYSQL_URI')

sqlEngine = create_engine(mysql_uri, pool_recycle=3600)

print ('=== mysql uri: ' + mysql_uri)

# rest  api
@application.route('/')
def hello():
    executor.submit(threaded_task,'data')
    return b'fun '

if __name__ == '__main__':

    application.run()

def threaded_task(data):  
    try:

        print ('===== run task')

        # load imbalance2  from  mysql 
        data_1 = pd.read_sql("select * from  TABLE 1 ", con=sqlEngine, index_col=None)

        print ('data1 to dataframe')

        data_2 = fun(data_1)

        # dataframe to  mysql 
        data_2.to_sql('TABLE 2', con=sqlEngine, if_exists='append', index=False)
        

    except Exception as e:
        print ('===error===')
        print (e)
        raise e
    return True
