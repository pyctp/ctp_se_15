#!/usr/bin/env python
#  -*- coding: utf-8 -*-
__author__ = 'chengzhi'

from datetime import datetime
from contextlib import closing
from tqsdk import TqApi, TqSim
from tqsdk.tools import DataDownloader
import sys
sys.path.append("..")
from getInstrumentsFromJsonFile import get_inst_info

import getopt, os, sys, re
import json


# 下载从 2018-01-01 到 2018-06-01 的 cu1805,cu1807,IC1803 分钟线数据，所有数据按 cu1805 的时间对齐
# 例如 cu1805 夜盘交易时段, IC1803 的各项数据为 N/A
# 例如 cu1805 13:00-13:30 不交易, 因此 IC1803 在 13:00-13:30 之间的K线数据会被跳过
#

def incept_config():
    config = {
        'output_dir': os.getcwd()
    }

    pattern = re.compile(r'\D*')

    def usage():
        print(("Usage:%s [-s] [--data_source]" % sys.argv[0]))
        # print "-s --data_source, is the path of data file."
        print("-o --output_dir, is the output directory. optional.")
        print("-n --name, is the instrument id. optional.")
        print("-g --granularities, default are 2,5,10,30,60 minutes, delimiter is a comma. optional.")
        print("-b --begin, default is HOLIDAYS first element, format is YYYY-MM-DD. optional.")
        print("-e --end, default is today, format is YYYY-MM-DD. optional.")
        print("-t --offset, default is 0, unit is second. optional.")

    opts = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:o:n:g:b:e:t:',
                                   ['help', 'data_source=', 'output_dir=', 'name=', 'granularities=', 'begin=', 'end=',
                                    'offset='])
    except getopt.GetoptError as e:
        print((str(e)))
        usage()
        exit(e.message.__len__())

    for k, v in opts:
        if k in ("-h", "--help"):
            usage()
            exit()

        elif k in ("-s", "--data_source"):
            config['data_source'] = v

        elif k in ("-o", "--output_dir"):
            config['output_dir'] = v

        elif k in ("-n", "--name"):
            config['name'] = v

        elif k in ("-g", "--granularities"):
            config['granularities'] = v

        elif k in ("-b", "--begin"):
            config['begin'] = v

        elif k in ("-e", "--end"):
            config['end'] = v

        elif k in ("-t", "--offset"):
            config['offset'] = int(v)

        else:
            print("unhandled option")

    if 'name' not in config:
        print('Must specify the -n (instrument name) arguments.')
        usage()
        exit(0)

    if 'granularities' not in config:
        inst_interval = 60
        config['output_file'] = '_'.join([os.path.basename(
            config['data_source']).split('_')[0], str(inst_interval)]) + '.json'

    return config


# instrumentid = "KQ.i@SHFE.rb"
# klinefile ='rb0000.csv'

def run(instrumentid, period, exchangeid='SHFE'):
    api = TqApi(TqSim())
    inst = instrumentid
    instinfo = get_inst_info(inst)
    exchangeid=instinfo['ExchangeID']
    period = int(period) if period is not None else 780

    instid = ''.join([exchangeid, '.', inst])
    datafile = inst + '_' + str(period) + '.csv'
    enddt = datetime.now()
    kd = DataDownloader(api, symbol_list=[instid], dur_sec=period,
                        start_dt=datetime(2016, 1, 1), end_dt=enddt, csv_file_name=datafile)

    with closing(api):
        while not kd.is_finished():
            api.wait_update()
            print(("progress: kline: %.2f%%" % kd.get_progress()))
        return datafile


def tq_to_awp(infile):
    klines = list()

    outfile = infile.split('.')[0] + '.json'

    with open(infile) as f:
        for i, line in enumerate(f):

            # 忽略 csv 头
            if i == 0:
                continue
            # 读取数据行，赋值给变量
            row = line.split(',')

            # print date_time, topen, thigh, tlow, tclose
            # 转换为字典格式
            k_line = {'open': float(row[1]), 'high': float(row[2]), 'low': float(row[3]), 'close': float(row[4]),
                      'date_time': row[0]}

            # append到k线列表中
            klines.append(k_line)

            # 查看格式是否正确
            # print date_time, topen, thigh, tlow, tclose
            # print len(klines)

    with open(outfile, 'a') as f:
        for kline in klines:
            f.writelines(json.dumps(kline, ensure_ascii=False) + '\n')
        f.close()


config = incept_config()
print((config['name'], config['granularities']))

infile = run(instrumentid=config['name'], period=config['granularities'])
print('download complete, begin transfer...')

tq_to_awp(infile=infile)

print('transfer complete...')
