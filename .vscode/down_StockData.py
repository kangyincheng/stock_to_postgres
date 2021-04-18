import sqlite3
import pandas as pd
import numpy as np
import requests
import re
import os
import time
import datetime
import tushare as ts
from sqlalchemy import create_engine
import psycopg2
class Jeoj:
    def __init__(self):
        try:
            os.chdir(r'E:\study_python\DB')#数据存储的数径
            ts.set_token('1527802b6802d5ae2d434e0f05add103c9e90b0cd32db5f68c959068')#用户token---没有的去tushare注册就有了
            self.pro = ts.pro_api()#实例化
            self.DB_name='postgres'
            self.Table_Name='postgres'
            #self.Table_Name='KANG'
            self.engine = create_engine(f'postgresql+psycopg2://postgres:SY639000@localhost:5432/postgres')
            #self.conn = self.engine.raw_connection()
            #self.cursor=self.conn.cursor()
            print("数据库连接成功")
        except Exception as e:
            print(f"数据库连接失败：{e}")
    def wrapFun(func):
        def inner(self,*args,**kwargs):
            try:
                print(f"本次执行的命令为：【{func.__name__}】;表名为：【{self.Table_Name}】")
                self.conn = self.engine.raw_connection()
                self.cursor=self.conn.cursor()
                ru=func(self,*args,**kwargs)
                self.conn.commit()
                self.cursor.close()
                self.conn.close()
                print(f'【{func.__name__}】命令执行成功')
                return ru
            except Exception as e:
                print(f"【{func.__name__}】命令执行失败：{e}")
                self.cursor.close()
                self.conn.close()
        return inner 
    @wrapFun
    def Create_Table(self):  
        # 创建表的SQL语句，默认编码为UTF-8
        SQL = f'''CREATE TABLE IF NOT EXISTS  {self.Table_Name} 
        (
        ts_code VARCHAR(20),
        trade_date DATE,
        open DECIMAL,
        high DECIMAL,
        low DECIMAL,
        close DECIMAL,
        pre_close DECIMAL,
        change DECIMAL,
        pct_chg DECIMAL,
        vol DECIMAL,
        amount DECIMAL
        );'''
        self.cursor.execute(SQL)


    @wrapFun
    def get_count(self):
        #检查数据库中表是否为空
        cursor=self.cursor
        cursor.execute(f"select count(*) from {self.Table_Name} ")
        Nb=cursor.fetchall()
        print(f'the count number of {self.Table_Name}is ',Nb)
        return  Nb[0][0]
    @wrapFun
    def db_time(self):
        #找出数据库最近的时间，并增加一天
        cursor=self.cursor
        sql_max=f'select max("trade_date") from {self.Table_Name}'
        cursor.execute(sql_max)
        da=cursor.fetchall()
        #print('da is ',da[0])
        if da[0]!=(None,):
            ds=datetime.datetime.strptime(str(da[0][0]),'%Y-%m-%d')+datetime.timedelta(days=1)
            ddt=ds.strftime('%Y%m%d')  
        else:
            ddt='19820101'
        return ddt
    @wrapFun
    def GetTables(self):
        #来检查数据库中是否有这个表
        cursor=self.cursor
        cursor.execute("select * from pg_tables where schemaname = 'public'")
        Tb=cursor.fetchall()
        if Tb:
            return True  
        else: 
            return False
    @wrapFun   
    def data_down_to_DB(self):        
        cursor = self.cursor#建立游标，通过此命令来实例化
        if self.GetTables()==False:#如果数据库是空的，将下载全部数据
            self.Create_Table()
            start='19820101'
        elif self.get_count()==0:
            start='19820101'
        else:#如果已有下载的数据，将在原有基础上更新
            start=self.db_time()
        end=datetime.datetime.now().strftime('%Y%m%d')#更新时所在的日期
        print('start is :',start,'; end is :',end)
        exchange_date=self.pro.trade_cal(exchange='',start_date=start,end_date=end)#调用TUSHARE中的交易日历
        df_date=exchange_date[exchange_date['is_open']==1]#找出开是的日期
        count=0
        for i in df_date['cal_date']:
            #此为进度条，只为看起去更直观些， 没有此循环，也不影响下载。
            count+=1
            time.sleep(0.1)
            df = self.pro.daily(trade_date=i)
            df.to_sql(f'{self.Table_Name}', self.engine, if_exists='append',index=False)  
            dis=int(50*count/len(df_date))
            print('\r','■'*dis+'□'*(50-dis),'{:.2%}  Date:{}'.format(count/len(df_date),i),end='')
        print('\n','【data_down_to_DB】股票数据下载已全部完成')
    @wrapFun
    def look_for_std(self,Name="close",Date="19820101"):
        #从数据库中读取数据到内存
        cursor = self.cursor#建立游标，通过此命令来实例化
        sql_std=f'select "ts_code" as code, stddev({Name}) as std_dev from {self.Table_Name} where "trade_date" >\'{Date}\' group by "ts_code"  order by std_dev desc'
        cursor.execute(sql_std)
        da=cursor.fetchall()
        df_db=pd.DataFrame(da,columns=['ts_code',f'{Name}_std'])
        return df_db
    @wrapFun
    def look_for_data(self,*args,Code="ts_code",DateName="trade_date",Date="20210410" ):
        #从数据库中读取数据到内存
        cursor = self.cursor#建立游标，通过此命令来实例化   
        col=list(args)+[Code,DateName]
        ab='","'.join(col)
        ag=f'"{ab}"'
        sql=f'select {ag} from {self.Table_Name}  where "trade_date" >\'{Date}\' '
        print('正在读取数据库中的数据，电脑配置的高低决定所用时间的长短，请喝杯茶,！--------耐心等待--------')    
        cursor.execute(sql)
        da=cursor.fetchall()
        df_db=pd.DataFrame(da,columns=col)
        return df_db


www=Jeoj()
#www.get_count()#返回表中的总行数
#www.db_time()#返回数据库上次更新的时间
#www.GetTables()#查看数据库的表晨面，是否已有我们要建的表，有返回True, 无返回False
#www.data_down_to_DB()#下载数据到本地数据库
www.look_for_std(Name="close",Date="20200101")#从本地数据库读取数据，并算出某一字段（列）的标准差
#www.look_for_data('close','open',Code='ts_code',DateName='trade_date',Date="19820101")#此处使用不定长参数，除有=号的外，其它可以传入一个或多个（注意不要与有=号的有复）