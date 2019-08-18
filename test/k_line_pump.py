#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time
import json


__author__ = 'James Iter'
__date__ = '2018/3/27'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2018 by James Iter.'


class KLinePump(object):

    def __init__(self):
        self.k_line = dict()
        self.str_k_line = None
        # interval: 单位(秒)
        self.interval = 60
        self.last_ts_step = None

    def process_data(self, depth_market_data=None, save_path=None):
        """
        :param depth_market_data:
        :param save_path: 文件存储路径
        :return:
        """

        assert isinstance(save_path, basestring)

        for key in ['last_price', 'trading_day', 'update_time']:
            if key not in depth_market_data:
                return

        trading_day = depth_market_data['trading_day']
        update_time = depth_market_data['update_time']
        date_time = ' '.join([depth_market_data['trading_day'], depth_market_data['update_time']])
        # ts_step = int(time.mktime(time.strptime(date_time, "%Y%m%d %H:%M:%S"))) / self.interval
        ts_step = int(time.mktime((int(trading_day[:4]), int(trading_day[4:6]), int(trading_day[6:]),
                                   int(update_time[:2]), int(update_time[3:5]), int(update_time[6:]),
                                   0, 0, 0))) / self.interval

        if self.last_ts_step is None:
            self.last_ts_step = ts_step

        if self.last_ts_step != ts_step:
            # 此处可以处理一些边界操作。比如对上一个区间的值做特殊处理等。

            with open(save_path, 'a') as f:
                self.str_k_line = json.dumps(self.k_line, ensure_ascii=False)
                f.writelines(self.str_k_line + '\n')

            self.last_ts_step = ts_step
            self.k_line = dict()

        last_price = depth_market_data['last_price']

        if 'open' not in self.k_line:
            self.k_line = {
                'open': last_price,
                'high': last_price,
                'low': last_price,
                'close': last_price,
                'date_time': date_time
            }

        self.k_line['close'] = last_price

        if last_price > self.k_line['high']:
            self.k_line['high'] = last_price

        elif last_price < self.k_line['low']:
            self.k_line['low'] = last_price

        else:
            pass

