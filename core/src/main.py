import talib as ta
import numpy as np
import tushare as ts
import matplotlib.pyplot as plt
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import boto3

AWS_REGION = 'us-east-1'
S3_BUCKET_NAME = 'i365.tech'
S3_DOUBLE_MA_BASE_DIR = 'invest-alchemy/data/strategy/double-ma/'
OUTPUT_FILE = datetime.today().strftime('%Y%m%d') + '.txt'
SNS_TOPIC = 'arn:aws:sns:us-east-1:745121664662:trade-signal-topic'

TODAY_STR = datetime.today().strftime('%Y%m%d')

pro = ts.pro_api(os.environ['TUSHARE_API_TOKEN'])

start = (datetime.today() - timedelta(days=150)).strftime('%Y%m%d')
end = TODAY_STR

short_term = 11
long_term = 22

buy_codes = []
sell_codes = []
hold_codes = []
empty_codes = []

class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open('/tmp/' + OUTPUT_FILE, "w")
        self.is_open = True

    def open(self):
        self.terminal = sys.stdout
        self.log = open('/tmp/' + OUTPUT_FILE, "w")
        self.is_open = True

    def close(self):
        self.log.close()
        self.is_open = False

    def write(self, message):
        self.terminal.write(message)
        if self.is_open:
            self.log.write(message)

    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        pass

sys.stdout = Logger()

def run(code, name):
    """
    fund_adj：获取基金复权因子，用于计算基金复权行情
    结果如下：
         ts_code    trade_date  adj_factor
0    513100.SH   20190926         1.0
1    513100.SH   20190925         1.0
2    513100.SH   20190924         1.0
    """
    adj = pro.fund_adj(ts_code=code, start_date=start, end_date=end).sort_index(ascending=False)

    """
    fund_daily：获取场内基金日线行情，类似股票日行情
    结果如下：
    ts_code     trade_date  pre_close   open   high    low  close  change  pct_change    vol      amount  
0    150008.SZ   20181029      1.070  0.964  1.070  0.964  1.070   0.000   0.0000        5.63       0.560  
1    150009.SZ   20181029      0.909  0.902  0.917  0.890  0.917   0.008   0.8801     1301.00     116.736 
2    150012.SZ   20181029      1.073  1.071  1.074  1.071  1.073   0.000   0.0000     2914.00     312.377  
3    150013.SZ   20181029      1.317  1.340  1.340  1.205  1.299  -0.018   -1.3667      82.00     9.903  
    """
    data = pro.fund_daily(ts_code=code, start_date=start, end_date=end).sort_index(ascending=False)

    # pandas函数merge：根据trade_date列合并两个结果
    data = data.merge(adj, on='trade_date')
    try:
        # pandas函数iloc：通过行号来取行数据（iloc[-1]数据中的最后一行）
        # 后复权计算：以除权前最后一天的价格点为基础把除权后的数据进行复权
        # 后复权将分红送股后的价格整体上移，所以最早交易日(4月2日)的价格是不变的，收盘价等于后复权收盘价：
        # 当天不复权收盘价 * 复权因子 / 前一天的复权因子
        qfq_close_price = data['close'].multiply(data['adj_factor']) / data['adj_factor'].iloc[-1]
    except:
        print("ERROR: cannot get price by ts, the code is " + code + " and will be skiped...\n")
        return
    # 复权收盘价
    close_price = np.array(qfq_close_price)
    # 交易日期
    time = np.array(data['trade_date'])
    short_ma = np.round(ta.MA(close_price, short_term), 3)
    long_ma = np.round(ta.MA(close_price, long_term), 3)
    if (short_ma[-1] > long_ma[-1] and short_ma[-2] <= long_ma[-2]):
        s = name + "(" + str(code) + ")" + "可买, 收盘价" + str(close_price[-1]) + ", 11日均线" + str(short_ma[-1]) + ", 22日均线" + str(long_ma[-1])
        buy_codes.append(s)
    elif (short_ma[-1] < long_ma[-1] and short_ma[-2] >= long_ma[-2]):
        s = name + "(" + str(code) + ")" + "可卖, 收盘价" + str(close_price[-1]) + ", 11日均线" + str(short_ma[-1]) + ", 22日均线" + str(long_ma[-1])
        sell_codes.append(s)
    else:
        if short_ma[-1] > long_ma[-1]:
            for i in range(1, len(long_ma)):
                if short_ma[-1 - i] <= long_ma[-1 - i]:
                    s = datetime.strptime(time[-1 - i], '%Y%m%d')
                    e = datetime.strptime(time[-1], '%Y%m%d')
                    interval_days = (e - s).days
                    s = name + "(" + str(code) + ")" + "于" + str(time[-1 - i]) + "日买入, " + "持有" + str(interval_days) + "天, 盈利" + str(round((close_price[-1] - close_price[-1 -i]) / close_price[-1 -i] * 100, 2)) + "%"
                    hold_codes.append(s)
                    break
        if short_ma[-1] < long_ma[-1]:
            for i in range(1, len(long_ma)):
                if short_ma[-1 - i] >= long_ma[-1 - i]:
                    s = datetime.strptime(time[-1 - i], '%Y%m%d')
                    e = datetime.strptime(time[-1], '%Y%m%d')
                    interval_days = (e - s).days
                    s = name + "(" + str(code) + ")" + "于" + str(time[-1 - i]) + "日卖出, " + "空仓" + str(interval_days) + "天, 空仓期涨幅" + str(round((close_price[-1] - close_price[-1 -i]) / close_price[-1 -i] * 100, 2)) + "%"
                    empty_codes.append(s)
                    break

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/plain; charset=utf-8'})
    except ClientError as e:
        logging.error(e)
        return False
    return True

def send_sns(file_name):
    """Send message by sns
    Amazon SNS is designed to distribute notifications, If you wish to send formatted emails, consider using Amazon Simple Email Service (SES), which improves email deliverability.
    """
    subject = 'Double MA Strategy Trade Signal: ' + TODAY_STR + ' - A Share'
    with open(file_name, 'r') as file:
        message = file.read()
        sns_client = boto3.client('sns', region_name=AWS_REGION)
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Message=message,
            Subject=subject,
        )

def make_trade_signal():
    print('================Long ETF=================\n')

    with open("best_etf.txt", "r") as f:
        for line in f:
            code_name = line.split(",")
            run(code_name[0], code_name[1].rstrip())
    
    print('可买标的:')
    for code in buy_codes:
        print(code)
    print('#########################################')
    print('可卖标的:')
    for code in sell_codes:
        print(code)
    print('#########################################')
    print('持仓标的:')
    for code in hold_codes:
        print(code)
    print('#########################################')
    print('空仓标的:')
    for code in empty_codes:
        print(code)

    # start fund.txt
    print('\n\n================Other ETF================\n')

    with open("fund.txt", "r") as f:
        for line in f:
            code_name = line.split(",")
            run(code_name[0], code_name[1].rstrip())
    
    print('可买标的:')
    for code in buy_codes:
        print(code)
    print('#########################################')
    print('可卖标的:')
    for code in sell_codes:
        print(code)
    print('#########################################')
    print('持仓标的:')
    for code in hold_codes:
        print(code)
    print('#########################################')
    print('空仓标的:')
    for code in empty_codes:
        print(code)

if __name__ == "__main__":
    make_trade_signal()
    sys.stdout.close()
    print('\nstart upload output file to s3...\n')
    upload_file('/tmp/' + OUTPUT_FILE, S3_BUCKET_NAME, S3_DOUBLE_MA_BASE_DIR + OUTPUT_FILE)
    print('end upload output file to s3\n')
    print('start send sns...\n')
    send_sns('/tmp/' + OUTPUT_FILE)
    print('end send sns...\n')
