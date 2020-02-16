#encoding:utf-8
#bar data file transfer: from tianqin to awp

import json

infile = 'rb1905_60.csv'
outfile = 'rb1905_60.json'
klines = list()

with open(infile) as f:
    for i, line in enumerate(f):

        # 忽略 csv 头
        if i == 0:
            continue
        #读取数据行，赋值给变量
        row = line.split(',')

        #print date_time, topen, thigh, tlow, tclose
        #转换为字典格式
        k_line = {'open': float(row[1]), 'high': float(row[2]), 'low': float(row[3]), 'close': float(row[4]),'date_time': row[0]}

        #append到k线列表中
        klines.append(k_line)

        # 查看格式是否正确
        # print date_time, topen, thigh, tlow, tclose
        # print len(klines)

with open(outfile,'a') as f:
    for kline in klines:
        f.writelines(json.dumps(kline, ensure_ascii=False) + '\n')
    f.close()





    '''
    天勤格式：
    datetime, SHFE.rb1905.open, SHFE.rb1905.high, SHFE.rb1905.low, SHFE.rb1905.close
    2018-05-15  20:48:00, 3420.0, 3430.0, 3420.0, 3427.0
    2018-05-15  21:01:00, 3427.0, 3430.0, 3422.0, 3429.0
    
    awp格式：
    {"high": 3430.0, "close": 3427.0, "open": 3420.0, "low": 3420.0, "date_time": "2018-05-15 20:48:00"}
    {"high": 3430.0, "close": 3429.0, "open": 3427.0, "low": 3422.0, "date_time": "2018-05-15 21:01:00"}
    '''
