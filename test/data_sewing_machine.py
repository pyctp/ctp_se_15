#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import sys
import getopt
import json
import time
import re
import Queue

from k_line_pump import KLinePump
from ma_pump import MAPump
from data_converter import trading_time_filter
from trading_period import TradingPeriod, EXCHANGE_TRADING_PERIOD, HOLIDAYS


__author__ = 'James Iter'
__date__ = '2018/4/6'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2018 by James Iter.'


DEPOSITARY_OF_KLINE = dict()
instrument_id_interval_pattern = re.compile(r'(\D*\d+)_(\d+)\.json')
contract_code_pattern = re.compile(r'\D*')
q_macs = Queue.Queue()


def incept_config():
    _config = {
        'granularities': '1,2,5,10,30,60',
        'ma_steps': '2,5,10',
        'macs': '2c5,5c10',
        'config_file': './data_sewing_machine.config'
    }

    def usage():
        print "Usage:%s [-s] [--data_source_dir]" % sys.argv[0]
        print "-s --data_source_dir, is the path of data file directory."
        print "-g --granularities, default are 2,5,10,30,60 minutes, delimiter is a comma. optional."
        print "-c --config. optional."

    opts = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:b:e:g:c:m:x:',
                                   ['help', 'data_source_dir=', 'begin=', 'end=', 'granularities=', 'config=',
                                    'ma_steps=', 'macs='])
    except getopt.GetoptError as e:
        print str(e)
        usage()
        exit(e.message.__len__())

    for k, v in opts:
        if k in ("-h", "--help"):
            usage()
            exit()

        elif k in ("-s", "--data_source_dir"):
            _config['data_source_dir'] = v

        elif k in ("-b", "--begin"):
            _config['begin'] = v

        elif k in ("-e", "--end"):
            _config['end'] = v

        elif k in ("-g", "--granularities"):
            _config['granularities'] = v

        elif k in ("-c", "--config"):
            _config['config_file'] = v

        elif k in ("-m", "--ma_steps"):
            _config['ma_steps'] = v

        elif k in ("-x", "--macs"):
            _config['macs'] = v

        else:
            print "unhandled option"

    if 'config_file' in _config:
        with open(_config['config_file'], 'r') as f:
            _config.update(json.load(f))

    if 'data_source_dir' not in _config:
        print 'Must specify the -s(data_source_dir) arguments.'
        usage()
        exit(1)

    if 'begin' not in _config:
        _config['begin'] = HOLIDAYS[0]

    if 'end' not in _config:
        _config['end'] = time.strftime('%Y-%m-%d')

    for granularity in _config['granularities'].split(','):

        if not isinstance(_config['granularities'], list):
            _config['granularities'] = list()

        # 忽略非整数的粒度
        if not granularity.isdigit():
            continue

        else:
            granularity = int(granularity)

        # 粒度小于 1 分钟，或大于 3600 分钟的不予支持
        if 1 > granularity > 3600:
            continue

        _config['granularities'].append(granularity)

    for ma_step in _config['ma_steps'].split(','):

        if not isinstance(_config['ma_steps'], list):
            _config['ma_steps'] = list()

        # 忽略非整数的粒度
        if not ma_step.isdigit():
            continue

        else:
            ma_step = int(ma_step)

        # 粒度小于 2，或大于 100 的不予支持
        if 2 > ma_step > 100:
            continue

        _config['ma_steps'].append(ma_step)

    for mac in _config['macs'].split(','):

        if not isinstance(_config['macs'], list):
            _config['macs'] = list()

        _config['macs'].append(mac)

    return _config


def load_data_from_file(instruments_id=None, granularities=None):

    files_name = list()

    if instruments_id is not None and granularities is not None:
        for instrument_id in instruments_id.split(','):
            for granularity in granularities.split(','):

                file_name = '_'.join([instrument_id, (60 * int(granularity)).__str__()]) + '.json'

                if os.path.isfile(os.path.join(config['data_source_dir'], file_name)):
                    files_name.append(file_name)

    else:
        for file_name in os.listdir(config['data_source_dir']):
            files_name.append(file_name)

    for file_name in files_name:
        p = instrument_id_interval_pattern.match(file_name)

        if p is not None:
            fields = p.groups()

            if fields[0] not in DEPOSITARY_OF_KLINE:
                DEPOSITARY_OF_KLINE[fields[0]] = dict()

            if fields[1] not in DEPOSITARY_OF_KLINE[fields[0]]:
                DEPOSITARY_OF_KLINE[fields[0]][fields[1]] = {
                    'path': os.path.join(config['data_source_dir'], file_name),
                    'data': list(),
                    'MA': dict(),
                    'MAC': dict()
                }

                for step in config['ma_steps']:
                    str_step = step.__str__()
                    if str_step not in DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MA']:
                        DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MA'][str_step] = dict()
                        DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MA'][str_step]['pump'] = MAPump(step=step)
                        DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MA'][str_step]['data'] = list()

                for mac in config['macs']:
                    if mac not in DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MAC']:
                        DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MAC'][mac] = dict()
                        DEPOSITARY_OF_KLINE[fields[0]][fields[1]]['MAC'][mac]['data'] = list()

    for k, v in DEPOSITARY_OF_KLINE.items():

        for _k, _v in v.items():
            with open(_v['path'], 'r') as f:
                for line in f:
                    json_k_line = json.loads(line.strip())
                    DEPOSITARY_OF_KLINE[k][_k]['data'].append(json_k_line)

                    for ma_k, ma_v in _v['MA'].items():
                        ma_ret = DEPOSITARY_OF_KLINE[k][_k]['MA'][ma_k]['pump'].process_data(json_k_line)
                        DEPOSITARY_OF_KLINE[k][_k]['MA'][ma_k]['data'].append(ma_ret)

                    for mac_k, mac_v in _v['MAC'].items():
                        mac = mac_k.lower().split('c')

                        data = {
                            'date_time': DEPOSITARY_OF_KLINE[k][_k]['MA'][mac[0]]['data'][-1]['date_time'],
                            'up_crossing': None,
                            'a': DEPOSITARY_OF_KLINE[k][_k]['MA'][mac[0]]['data'][-1]['avg'],
                            'b': DEPOSITARY_OF_KLINE[k][_k]['MA'][mac[1]]['data'][-1]['avg']
                        }

                        last_mac = {
                            'up_crossing': data['a'] >= data['b']
                        }

                        if DEPOSITARY_OF_KLINE[k][_k]['MAC'][mac_k]['data'].__len__() > 0:
                            last_mac = DEPOSITARY_OF_KLINE[k][_k]['MAC'][mac_k]['data'][-1]

                        if data['a'] > data['b']:

                            data['up_crossing'] = True

                        elif data['a'] < data['b']:

                            data['up_crossing'] = False

                        else:
                            data['up_crossing'] = None

                        if DEPOSITARY_OF_KLINE[k][_k]['MAC'][mac_k]['data'].__len__() > 0 and \
                                last_mac['up_crossing'] == data['up_crossing']:
                            data['up_crossing'] = None

                        DEPOSITARY_OF_KLINE[k][_k]['MAC'][mac_k]['data'].append(data)
                        q_macs.put({'instrument_id': k, 'granularity': _k, 'mac_k': mac_k, 'data': data})


def init_k_line_pump():

    for k, v in DEPOSITARY_OF_KLINE.items():
        for _k, _v in v.items():
            DEPOSITARY_OF_KLINE[k][_k]['k_line_pump'] = KLinePump()
            DEPOSITARY_OF_KLINE[k][_k]['k_line_pump'].interval = int(_k)


config = incept_config()
# load_data_from_file(instruments_id='rb1805', granularities='1,2,5')
# init_k_line_pump()

# workdays = TradingPeriod.get_workdays(begin=config['begin'], end=config['end'])
# workdays_exchange_trading_period_by_ts = \
#     TradingPeriod.get_workdays_exchange_trading_period(
#         _workdays=workdays, exchange_trading_period=EXCHANGE_TRADING_PERIOD)


def sewing_data_to_file_and_depositary(depth_market_data=None):

    # for key in ['InstrumentID', 'LastPrice', 'ActionDay', 'UpdateTime']:
    #     if not hasattr(depth_market_data, key):
    #         return

    instrument_id = depth_market_data.InstrumentID
    contract_code = contract_code_pattern.match(instrument_id).group()

    # workdays = TradingPeriod.get_workdays(begin=config['begin'], end='2019-12-31')
    # workdays_exchange_trading_period_by_ts = \
    #     TradingPeriod.get_workdays_exchange_trading_period(
    #         _workdays=workdays, exchange_trading_period=EXCHANGE_TRADING_PERIOD)

    date = '-'.join([depth_market_data.ActionDay[:4], depth_market_data.ActionDay[4:6],
                     depth_market_data.ActionDay[6:]])

    date_time = ' '.join([date, depth_market_data.UpdateTime])

    # if not trading_time_filter(
    #         date_time=date_time, contract_code=contract_code,
    #         exchange_trading_period_by_ts=workdays_exchange_trading_period_by_ts[date]):
    #     return

    formatted_depth_market_data = dict()
    formatted_depth_market_data['trading_day'] = date.replace('-', '')
    formatted_depth_market_data['update_time'] = depth_market_data.UpdateTime
    formatted_depth_market_data['instrument_id'] = instrument_id

    if isinstance(depth_market_data.LastPrice, basestring):
        if depth_market_data.LastPrice.isdigit():
            formatted_depth_market_data['last_price'] = int(depth_market_data.LastPrice)
        else:
            try:
                formatted_depth_market_data['last_price'] = float('%0.2f' % float(depth_market_data.LastPrice))
            except ValueError:
                return

    else:
        formatted_depth_market_data['last_price'] = depth_market_data.LastPrice

    if instrument_id not in DEPOSITARY_OF_KLINE:
        DEPOSITARY_OF_KLINE[instrument_id] = dict()

    for granularity in config['granularities']:
        interval = 60 * granularity
        str_interval = str(interval)

        if str_interval not in DEPOSITARY_OF_KLINE[instrument_id]:
            file_name = '_'.join([instrument_id, str_interval]) + '.json'
            DEPOSITARY_OF_KLINE[instrument_id][str_interval] = {
                'path': os.path.join(config['data_source_dir'], file_name),
                'data': list()
            }
            DEPOSITARY_OF_KLINE[instrument_id][str_interval]['k_line_pump'] = KLinePump()
            DEPOSITARY_OF_KLINE[instrument_id][str_interval]['k_line_pump'].interval = interval

    for k, v in DEPOSITARY_OF_KLINE[instrument_id].items():
        DEPOSITARY_OF_KLINE[instrument_id][k]['k_line_pump'].process_data(
            depth_market_data=formatted_depth_market_data, save_path=DEPOSITARY_OF_KLINE[instrument_id][k]['path'])

        if DEPOSITARY_OF_KLINE[instrument_id][k]['k_line_pump'].str_k_line is not None:
            json_k_line = json.loads(DEPOSITARY_OF_KLINE[instrument_id][k]['k_line_pump'].str_k_line)
            DEPOSITARY_OF_KLINE[instrument_id][k]['data'].append(json_k_line)

            DEPOSITARY_OF_KLINE[instrument_id][k]['k_line_pump'].str_k_line = None

            # if 'MA' not in DEPOSITARY_OF_KLINE[instrument_id][k]:
            #     DEPOSITARY_OF_KLINE[instrument_id][k]['MA'] = dict()
            #
            #     for step in config['ma_steps']:
            #         str_step = step.__str__()
            #         if str_step not in DEPOSITARY_OF_KLINE[instrument_id][k]['MA']:
            #             DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][str_step] = dict()
            #             DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][str_step]['pump'] = MAPump(step=step)
            #             DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][str_step]['data'] = list()

            # for ma_k, ma_v in DEPOSITARY_OF_KLINE[instrument_id][k]['MA'].items():
            #     ma_ret = DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][ma_k]['pump'].process_data(json_k_line)
            #     DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][ma_k]['data'].append(ma_ret)
            #
            # if 'MAC' not in DEPOSITARY_OF_KLINE[instrument_id][k]:
            #     DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'] = dict()
            #
            #     for mac in config['macs']:
            #         if mac not in DEPOSITARY_OF_KLINE[instrument_id][k]['MAC']:
            #             DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'][mac] = dict()
            #             DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'][mac]['data'] = list()
            #
            # for mac_k, mac_v in DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'].items():
            #     mac = mac_k.lower().split('c')
            #
            #     data = {
            #         'date_time': DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][mac[0]]['data'][-1]['date_time'],
            #         'up_crossing': None,
            #         'a': DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][mac[0]]['data'][-1]['avg'],
            #         'b': DEPOSITARY_OF_KLINE[instrument_id][k]['MA'][mac[1]]['data'][-1]['avg']
            #     }
            #
            #     last_mac = {
            #         'up_crossing': data['a'] >= data['b']
            #     }
            #
            #     if DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'][mac_k]['data'].__len__() > 0:
            #         last_mac = DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'][mac_k]['data'][-1]
            #
            #     if data['a'] > data['b']:
            #
            #         data['up_crossing'] = True
            #
            #     elif data['a'] < data['b']:
            #
            #         data['up_crossing'] = False
            #
            #     else:
            #         data['up_crossing'] = None
            #
            #     if DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'][mac_k]['data'].__len__() > 0 and \
            #             last_mac['up_crossing'] == data['up_crossing']:
            #         data['up_crossing'] = None
            #
            #     DEPOSITARY_OF_KLINE[instrument_id][k]['MAC'][mac_k]['data'].append(data)
            #     q_macs.put({'instrument_id': instrument_id, 'granularity': k, 'mac_k': mac_k, 'data': data})


def get_k_line_column(instrument_id=None, interval=None, ohlc='high', depth=0):
    """
    :param instrument_id: 合约名称。
    :param interval: 取样间隔。
    :param ohlc: [Open|High|Low|Close]。
    :param depth: 深度。默认 0 将获取所有。
    :return: list。
    """

    ohlc = ohlc.lower()

    assert ohlc in ['open', 'high', 'low', 'close']

    k_line_column = list()
    str_interval = str(interval)
    max_depth = DEPOSITARY_OF_KLINE[instrument_id][str_interval]['data'].__len__()
    if depth == 0 or depth >= max_depth:
        depth = max_depth

    depth = 0 - depth

    for i in range(depth, 0):
        k_line_column.append(DEPOSITARY_OF_KLINE[instrument_id][str_interval]['data'][i][ohlc])

    return k_line_column


def get_last_k_line(instrument_id=None, interval=None):
    """
    :param instrument_id: 合约名称。
    :param interval: 取样间隔。
    :return: dict。
    """

    str_interval = str(interval)
    if 1 > DEPOSITARY_OF_KLINE[instrument_id][str_interval]['data'].__len__():
        return None

    return DEPOSITARY_OF_KLINE[instrument_id][str_interval]['data'][-1]


def get_mac(instrument_id=None, interval=None, mac=None):

    if instrument_id not in DEPOSITARY_OF_KLINE:
        return None

    if interval not in DEPOSITARY_OF_KLINE[instrument_id]:
        return None

    if 'MAC' not in DEPOSITARY_OF_KLINE[instrument_id][interval]:
        return None

    if mac not in DEPOSITARY_OF_KLINE[instrument_id][interval]['MAC']:
        return None

    return DEPOSITARY_OF_KLINE[instrument_id][interval]['MAC'][mac]['data']


def hhv(series=None, step=None):

    assert isinstance(series, list)
    assert 0 < step <= series.__len__()

    numbers = list()
    stack = list()

    for number in series:
        stack.append(number)

        if stack.__len__() > step:
            stack = stack[0 - step:]

        numbers.append(max(stack))

    return numbers


def llv(series=None, step=None):

    assert isinstance(series, list)
    assert 0 < step <= series.__len__()

    numbers = list()
    stack = list()

    for number in series:
        stack.append(number)

        if stack.__len__() > step:
            stack = stack[0 - step:]

        numbers.append(min(stack))

    return numbers


def cross_up(series_a=None, series_b=None):

    assert isinstance(series_a, list)
    assert isinstance(series_b, list)
    assert series_a.__len__() == series_b.__len__()

    cross_series = list()

    for i, v in enumerate(series_a):
        if i > 0:
            if series_a[i] > series_b[i] and series_a[i - 1] <= series_b[i - 1]:
                cross_series.append(True)
                continue

        cross_series.append(None)

    return cross_series


def cross_down(series_a=None, series_b=None):

    assert isinstance(series_a, list)
    assert isinstance(series_b, list)
    assert series_a.__len__() == series_b.__len__()

    cross_series = list()

    for i, v in enumerate(series_a):
        if i > 0:
            if series_a[i] < series_b[i] and series_a[i - 1] >= series_b[i - 1]:
                cross_series.append(True)
                continue

        cross_series.append(None)

    return cross_series


def test():
    from collections import namedtuple

    DepthMarketData = namedtuple('DepthMarketData', 'ActionDay InstrumentID LastPrice UpdateTime')

    data_s = list()
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3445, UpdateTime='11:17:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3447, UpdateTime='11:17:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3443, UpdateTime='11:17:35'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3440, UpdateTime='11:17:36'))

    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3345, UpdateTime='11:18:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3347, UpdateTime='11:18:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3243, UpdateTime='11:18:35'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3450, UpdateTime='11:18:36'))

    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3245, UpdateTime='11:19:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3247, UpdateTime='11:19:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3243, UpdateTime='11:19:35'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3250, UpdateTime='11:19:36'))

    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3545, UpdateTime='11:20:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3347, UpdateTime='11:20:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3243, UpdateTime='11:20:35'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3400, UpdateTime='11:20:36'))

    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3145, UpdateTime='11:21:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3047, UpdateTime='11:21:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3643, UpdateTime='11:21:35'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3200, UpdateTime='11:21:36'))

    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3545, UpdateTime='11:22:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3047, UpdateTime='11:22:34'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3443, UpdateTime='11:22:35'))
    data_s.append(DepthMarketData(ActionDay='20180327', InstrumentID='rb1805', LastPrice=3500, UpdateTime='11:22:36'))

    for data in data_s:
        sewing_data_to_file_and_depositary(depth_market_data=data)

    new_k_line_col = get_k_line_column(instrument_id='rb1805', interval=120, ohlc='high', depth=10)

    pass


# test()
