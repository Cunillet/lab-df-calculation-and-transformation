from sqlalchemy import create_engine
import pandas as pd
import pymysql

class Connector:
    def __init__(self, connection_path):
        self.engine = create_engine(connection_path)
        self.conn = self.engine.connect()
    
    def read_sql(self, sql):
        return pd.read_sql(sql, self.conn)

    def to_sql(self, df, table_name, ifexists):
        df.to_sql(table_name, self.conn, if_exists=ifexists)

    def __del__(self):
        print('-----------------------')
        try:
            self.conn.close()
        except:
            print('could not close connection')
        try:
            self.engine.dispose()
        except:
            print('could not close engine')
        finally:
            print('Connector destroyed.')



def get_sales_amount_item(row):
    return row.item_price * row.item_cnt_day

def search_item_cnt_day_errors(df):
    print(df.describe())
    print('seems item cnt_day may even be -1. Probably is because of a return')
    print(df[df.item_cnt_day == -1].count())
    print(df.shape)
    print('30 values from 4545 can or can not be a mistake so, to prevent data lose will keep them')
    print('counting them as stores with returns but without sales from an item')
    print('---------------------------')
    print('item count max is 10, mean is 1.1 this doesn\'t seem to be ')
    print('---------------------------')
    print('a standard deviation of 0.5 when the mean is 1.1 and max is 10 doesn\'t seem to be out of range')

def search_item_price_errors(df):
    print(df.describe())
    print('min value', df.item_price.min())
    print('max value', df.item_price.max())
    bins = pd.cut(df.item_price, 4, labels=['0-25','25-50','50-75','75-100']).value_counts()
    print(bins)
    bins = pd.cut(df.item_price, 10, labels=['1','2','3','4','5','6','7','8','9','10']).value_counts()
    print(bins)
    print('when cutting doesn\'t seem to be any error of price values')
    print('there seems to be two separated ranges but 21 items over 80% doesn\'t seem to be an error')




driver = 'mysql+pymysql:'
user = 'data-students'
password = 'iR0nH@cK-D4T4B4S3'
ip = '34.65.10.136'
database = 'retail_sales'
conn = Connector(f'{driver}//{user}:{password}@{ip}/{database}')

df = conn.read_sql('SELECT * FROM raw_sales')

search_item_cnt_day_errors(df)
search_item_price_errors(df)

driver   = 'mysql+pymysql:'
user     = 'root'
password = '53cawentOo69'
ip       = 'localhost'
database = 'retail_sales'
local = Connector(f'{driver}//{user}:{password}@{ip}/{database}')

local.to_sql(df, 'raw_sales1', 'replace')

shop_df = df.copy()
shop_df['sales_per_item'] = shop_df.apply(lambda row : get_sales_amount_item(row), axis=1)
# SALES PER SHOP
sales_shop_df = shop_df.copy()
#sales_shop_df = sales_shop_df.groupby(by=['shop_id','date'])[['sales_per_item']].sum()
sales_shop_df = sales_shop_df.groupby(by=['shop_id']).agg({'sales_per_item':'sum'})
#print(sales_shop_df)
local.to_sql(sales_shop_df, 'sales_by_store1', 'replace')



# SALES PER ITEM
sales_item_df = shop_df.copy()
sales_item_df = sales_item_df.groupby(by=['item_id']).agg({'sales_per_item':'sum'})
#print(sales_item_df)
local.to_sql(sales_item_df, 'sales_By_item1', 'replace')
