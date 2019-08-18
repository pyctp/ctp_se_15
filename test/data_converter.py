#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import sys
import getopt
import json
from datetime import datetime
import time
import re

from trading_period import TradingPeriod, EXCHANGE_TRADING_PERIOD, FUTURES_TRADING_PERIOD_MAPPING, HOLIDAYS


__author__ = 'James Iter'
__date__ = '2018/4/1'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2018 by James Iter.'


def incept_config():
    config = {
        'output_dir': os.getcwd(),
        'granularities': '2,5,10,30,60'
    }

    pattern = re.compile(r'\D*')

    def usage():
        print "Usage:%s [-s] [--data_source]" % sys.argv[0]
        print "-s --data_source, is the path of data file."
        print "-o --output_dir, is the output directory. optional."
        print "-n --name, is the instrument id. optional."
        print "-g --granularities, default are 2,5,10,30,60 minutes, delimiter is a comma. optional."
        print "-b --begin, default is HOLIDAYS first element, format is YYYY-MM-DD. optional."
        print "-e --end, default is today, format is YYYY-MM-DD. optional."
        print "-t --offset, default is 0, unit is second. optional."

    opts = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:o:n:g:b:e:t:',
                                   ['help', 'data_source=', 'output_dir=', 'name=', 'granularities=', 'begin=', 'end=',
                                    'offset='])
    except getopt.GetoptError as e:
        print str(e)
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
            print "unhandled option"

    if 'data_source' not in config:
        print 'Must specify the -s(data_source) arguments.'
        usage()
        exit(1)

    if 'name' not in config:
        config['name'] = os.path.basename(config['data_source']).split('.')[0]

    config['contract_code'] = pattern.match(config['name']).group()

    for granularity in config['granularities'].split(','):

        if not isinstance(config['granularities'], list):
            config['granularities'] = list()

        # 忽略非整数的粒度
        if not granularity.isdigit():
            continue

        else:
            granularity = int(granularity)

        # 粒度小于 2 分钟，或大于 3600 分钟的不予支持
        if 2 > granularity > 3600:
            continue

        config['granularities'].append(granularity)

    if 'begin' not in config:
        config['begin'] = HOLIDAYS[0]

    if 'end' not in config:
        config['end'] = time.strftime('%Y-%m-%d')

    if 'offset' not in config:
        config['offset'] = 0

    return config


class DateConverter(object):

    def __init__(self):
        self.k_line = dict()
        # interval: 单位(秒)
        self.interval = 60
        self.last_ts_step = None
        self.name = None
        self.save_path = None
        self.is_tick = None

    def save_last(self):
        with open(self.save_path, 'a') as f:
            f.writelines(json.dumps(self.k_line, ensure_ascii=False) + '\n')

    def data_pump(self, depth_market_data=None, save_dir_path=None):
        """
        :param depth_market_data:
        :param save_dir_path: 文件存储目录路径
        :return:
        """
        for key in ['last_price', 'action_day', 'update_time']:
            if key not in depth_market_data:
                return

        action_day = depth_market_data['action_day']
        update_time = depth_market_data['update_time']

        origin_date_time = ' '.join([action_day, update_time])

        if self.is_tick is None:
            if update_time.find('.') != -1:
                self.is_tick = True

            else:
                self.is_tick = False

        if self.is_tick:
            dt = datetime.strptime(origin_date_time, "%Y%m%d %H:%M:%S.%f")
            ts = time.mktime(dt.timetuple()) + (dt.microsecond / 1e6)
            ts -= 0.1

        else:
            ts = int(time.mktime(time.strptime(origin_date_time, "%Y%m%d %H:%M:%S")))
            # ts = int(time.mktime((int(action_day[:4]), int(action_day[4:6]), int(action_day[6:]),
            #                       int(update_time[:2]), int(update_time[3:5]), int(update_time[6:8]),
            #                       0, 0, 0)))

        ts += self.interval
        ts_step = int(ts) / self.interval

        if self.last_ts_step is None:
            self.last_ts_step = ts_step

        if self.last_ts_step != ts_step:
            # 此处可以处理一些边界操作。比如对上一个区间的值做特殊处理等。

            if save_dir_path is not None:
                file_name = '_'.join([self.name, self.interval.__str__()]) + '.json'
                self.save_path = '/'.join([save_dir_path, file_name])

                if not os.path.isdir(save_dir_path):
                    os.makedirs(save_dir_path, 0755)

                with open(self.save_path, 'a') as f:
                    f.writelines(json.dumps(self.k_line, ensure_ascii=False) + '\n')

            self.last_ts_step = ts_step
            self.k_line = dict()

        last_price = depth_market_data['last_price']

        if 'open' not in self.k_line:
            self.k_line = {
                'open': last_price,
                'high': last_price,
                'low': last_price,
                'close': last_price,
                'date_time': time.strftime("%Y%m%d %H:%M:%S", time.localtime(ts_step * self.interval))
            }

        self.k_line['close'] = last_price

        if last_price > self.k_line['high']:
            self.k_line['high'] = last_price

        elif last_price < self.k_line['low']:
            self.k_line['low'] = last_price

        else:
            pass


def trading_time_filter(date_time=None, contract_code=None, exchange_trading_period_by_ts=None):
    is_tick = False
    if date_time.find('.') != -1:
        is_tick = True

    if is_tick:
        dt = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S.%f")
        ts = time.mktime(dt.timetuple()) + (dt.microsecond / 1e6)
    else:
        ts = int(time.mktime(time.strptime(date_time, "%Y-%m-%d %H:%M:%S")))

    contract_trading_period_ts = list()

    for trading_period in FUTURES_TRADING_PERIOD_MAPPING[contract_code]:
        if trading_period.exchange_code not in exchange_trading_period_by_ts:
            continue

        if trading_period.period not in exchange_trading_period_by_ts[trading_period.exchange_code]:
            continue

        contract_trading_period_ts.extend(
            exchange_trading_period_by_ts[trading_period.exchange_code][trading_period.period])

    for trading_period_ts in contract_trading_period_ts:
        if trading_period_ts[0] <= ts <= trading_period_ts[1]:
            return True

    return False


def run():
    config = incept_config()
    config['is_tick'] = None
    workdays = TradingPeriod.get_workdays(begin=config['begin'], end=config['end'])
    workdays_exchange_trading_period_by_ts = \
        TradingPeriod.get_workdays_exchange_trading_period(
            _workdays=workdays, exchange_trading_period=EXCHANGE_TRADING_PERIOD)

    date_converters = list()

    for granularity in config['granularities']:
        date_converter = DateConverter()
        date_converter.name = config['name']
        date_converter.interval = 60 * granularity
        date_converters.append(date_converter)

    lines = list()

    with open(config['data_source']) as f:
        for i, line in enumerate(f):

            # 忽略 csv 头
            if i == 0:
                continue

            lines.append(line)

    for i, line in enumerate(lines):

        if i % 10000 == 0:
            print ' '.join([time.strftime('%H:%M:%S'), i.__str__()])

        depth_market_data = dict()
        row = line.split(',')

        row[0] = row[0].replace('/', '-')

        if config['is_tick'] is None:
            if row[0].find('.') != -1:
                config['is_tick'] = True

            else:
                config['is_tick'] = False

        if config['offset'] > 0:
            if config['is_tick']:
                dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
                ts = time.mktime(dt.timetuple()) + (dt.microsecond / 1e6)
            else:
                ts = int(time.mktime(time.strptime(row[0], "%Y-%m-%d %H:%M:%S")))

            ts += config['offset']

            if config['is_tick']:
                row[0] = datetime.strftime(datetime.fromtimestamp(ts), "%Y-%m-%d %H:%M:%S.%f")
            else:
                row[0] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

        date_time = row[0].split(' ')

        if date_time[0] not in workdays_exchange_trading_period_by_ts:
            continue

        if not trading_time_filter(
                date_time=row[0], contract_code=config['contract_code'],
                exchange_trading_period_by_ts=workdays_exchange_trading_period_by_ts[date_time[0]]):
            continue

        depth_market_data['action_day'] = ''.join(date_time[0].split('-'))
        depth_market_data['update_time'] = date_time[1]

        if config['is_tick']:
            depth_market_data['last_price'] = row[1]

        else:
            depth_market_data['last_price'] = row[4]

        if depth_market_data['last_price'].isdigit():
            depth_market_data['last_price'] = int(depth_market_data['last_price'])
        else:
            try:
                depth_market_data['last_price'] = float('%0.2f' % float(depth_market_data['last_price']))
            except ValueError:
                continue

        for date_converter in date_converters:
            date_converter.data_pump(depth_market_data=depth_market_data, save_dir_path=config['output_dir'])

    for date_converter in date_converters:
        date_converter.save_last()


if __name__ == '__main__':
    run()

