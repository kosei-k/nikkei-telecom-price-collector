import requests
from bs4 import BeautifulSoup
import datetime as dt
from zoneinfo import ZoneInfo
import pandas as pd
import time
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
# ログの出力名を設定（1）
logger = logging.getLogger('LoggingTest')
 
# ログレベルの設定（2）
logger.setLevel(10)
 
# ログのコンソール出力の設定（3）
sh = logging.StreamHandler()
logger.addHandler(sh)
 
# ログのファイル出力先を設定（4）
fh = logging.FileHandler('./app/get_price_list.log')
logger.addHandler(fh)

# ログの出力形式の設定
formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
fh.setFormatter(formatter)
sh.setFormatter(formatter)

# バッチ実行日の取得
create_date = dt.datetime.now(ZoneInfo("Asia/Tokyo")).strftime('%Y-%m-%d %H:%M:%S')
print(create_date)
url = "https://www.traders.co.jp/market_jp/stock_list/price/all/information/1" 

response = requests.get(url)
response.encoding = response.apparent_encoding

bs = BeautifulSoup(response.text, 'html.parser')

# 最大ページ数の取得
links = bs.find_all('a', class_="page-link")
max_page = 0
for link in links:
    if link.text.isdigit():
        max_page = int(link.text)
time.sleep(1)

data_list = []
for page in range(1, max_page+1):
    url = "https://www.traders.co.jp/market_jp/stock_list/price/all/information/{page}".format(page=page) 
 
    response = requests.get(url)
    response.encoding = response.apparent_encoding
    
    bs = BeautifulSoup(response.text, 'html.parser')

    # ページ更新日の取得
    div_item = bs.find(name="div", class_="data_table_timestamp")
    page_update_date = div_item.text.replace("/", "-")

    tr_list = bs.find_all('tr')
    for i, tr in enumerate(tr_list):
        temp1 = tr.select('td.text-nowrap')
        if temp1:
            text = temp1[0].get_text(strip=True)
            if "(" in text:
                sp = text.split("(")
                name = sp[0]
                sp = sp[1].split("/")
                code = sp[0]
                sp = sp[1].split(")")
                market = sp[0]

                text = tr.select('td.text-right')[0].get_text().replace(",", "")
                price = None if text == "-" else float(text)
                data_list.append([name, code, market, price, page_update_date, create_date])
    time.sleep(1)

col_names=["NAME", "CODE", "MARKET", "PRICE", "PAGE_UPDATE_DATE", "CREATE_DATE"]
df = pd.DataFrame(data_list, columns=col_names)
df.to_csv(os.path.join("./app/csv", "price_list_" + create_date.split(" ")[0] + ".csv"), index=False)


try:
    logger.info('SF接続開始')
    ctx = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USERNAME'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        database="STOCK",
        warehouse="KIRYU_WH",
        schema="PUBLIC",
        )
    cs = ctx.cursor()
    logger.info('SF接続完了')

    logger.info('SFテーブル作成')
    res = ctx.cursor().execute(
    "CREATE TABLE IF NOT EXISTS "
    "STOCK_PRICE(name string, code string, market string, price NUMBER(8,1), page_update_date DATETIME, create_date DATETIME)")
    logger.debug(res)

    logger.info('SF書込開始')
    res = write_pandas(ctx, df, 'STOCK_PRICE')
    logger.debug(res)
    logger.info('SF書込終了')
except Exception as e:
    logger.error(e)
finally:
    cs.close()
ctx.close()
