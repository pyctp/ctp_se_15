#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 交易时段


import time
from collections import namedtuple


__author__ = 'James Iter'
__date__ = '2018/4/3'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2018 by James Iter.'


# 公共假日
HOLIDAYS = [
    "2016-12-31",
    "2017-01-01",
    "2017-01-02",

    "2017-01-27",
    "2017-01-28",
    "2017-01-29",
    "2017-01-30",
    "2017-01-31",
    "2017-02-01",
    "2017-02-02",

    "2017-04-01",
    "2017-04-02",
    "2017-04-03",
    "2017-04-04",

    "2017-04-29",
    "2017-04-30",
    "2017-05-01",

    "2017-05-27",
    "2017-05-28",
    "2017-05-29",
    "2017-05-30",

    "2017-09-30",
    "2017-10-01",
    "2017-10-02",
    "2017-10-03",
    "2017-10-04",
    "2017-10-05",
    "2017-10-06",
    "2017-10-07",
    "2017-10-08",

    "2017-12-30",
    "2017-12-31",
    "2018-01-01",

    "2018-02-15",
    "2018-02-16",
    "2018-02-17",
    "2018-02-18",
    "2018-02-19",
    "2018-02-20",
    "2018-02-21",

    "2018-04-05",
    "2018-04-06",
    "2018-04-07",
    "2018-04-08",

    "2018-04-28",
    "2018-04-29",
    "2018-04-30",
    "2018-05-01",

    "2018-06-16",
    "2018-06-17",
    "2018-06-18",

    "2018-09-22",
    "2018-09-23",
    "2018-09-24",

    "2018-09-29",
    "2018-09-30",
    "2018-10-01",
    "2018-10-02",
    "2018-10-03",
    "2018-10-04",
    "2018-10-05",
    "2018-10-06",
    "2018-10-07",
    "2018-12-30",

    "2019-02-04",
    "2019-02-05",
    "2019-02-06",
    "2019-02-07",
    "2019-02-08",
    "2019-02-09",
    "2019-02-10",

    "2019-04-05",
    "2019-04-06",
    "2019-04-07",

    "2019-05-01",

    "2019-06-07",
    "2019-06-08",
    "2019-06-09",

    "2019-09-13",
    "2019-09-14",
    "2019-09-15",

    "2019-10-01",
    "2019-10-02",
    "2019-10-03",
    "2019-10-04",
    "2019-10-05",
    "2019-10-06",
    "2019-10-07"

]

EXCHANGE_TRADING_PERIOD = {

    # 中国金融期货交易所 股指期货交易时间
    # http://www.cffex.com.cn
    'CFFEX_STOCK_INDEX': {
        'daytime': [('09:25:00', '11:30:00'), ('13:00:00', '15:00:00')]
    },

    # 中国金融期货交易所 国债期货交易时间
    # http://www.cffex.com.cn
    'CFFEX_NATIONAL_DEBT': {
        'daytime': [('09:10:00', '11:30:00'), ('13:00:00', '15:15:00')]
    },

    # 郑州商品交易所交易时间
    # http://www.czce.com.cn
    'CZCE': {
        'daytime': [('09:00:00', '10:15:01'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00')],
        'night': [('21:00:00', '23:30:00')]
    },

    # 上海期货交易所交易时间
    # http://www.shfe.com.cn
    'SHFE': {
        'daytime': [('09:00:00', '10:15:01'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00')],
        'night_group_01': [('21:00:00', '02:30:00')],
        'night_group_02': [('21:00:00', '01:00:00')],
        'night_group_03': [('21:00:00', '23:00:00')]
    },

    # 大连商品交易所交易时间
    # http://www.dce.com.cn
    'DCE': {
        'daytime': [('09:00:00', '10:15:01'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00')],
        'night': [('21:00:00', '23:30:00')]
    },

    # 上海国际能源交易所交易时间
    # http://www.ine.cn
    'INE': {
        'daytime': [('09:00:00', '10:15:01'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00')],
        'night': [('21:00:00', '02:30:00')]
    }
}

TradingPeriod = namedtuple('TradingPeriod', 'exchange_code period')

FUTURES_TRADING_PERIOD_MAPPING = {
    # 中金所
    "IC": [TradingPeriod(exchange_code='CFFEX_STOCK_INDEX', period='daytime')],     # 中证500股指
    "IF": [TradingPeriod(exchange_code='CFFEX_STOCK_INDEX', period='daytime')],     # 沪深300股指
    "IH": [TradingPeriod(exchange_code='CFFEX_STOCK_INDEX', period='daytime')],     # 上证50股指
    "TF": [TradingPeriod(exchange_code='CFFEX_NATIONAL_DEBT', period='daytime')],   # 5年国债
    "T": [TradingPeriod(exchange_code='CFFEX_NATIONAL_DEBT', period='daytime')],    # 10年国债

    # 郑商所
    "CF": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 棉花
    "ZC": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 动力煤
    "SR": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 白砂糖
    "RM": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 菜籽粕
    "MA": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 甲醇
    "TA": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # PTA化纤
    "FG": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 玻璃
    "OI": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 菜籽油
    "CY": [TradingPeriod(exchange_code='CZCE', period='daytime'),
           TradingPeriod(exchange_code='CZCE', period='night')],      # 棉纱
    "WH": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 强筋麦709
    "SM": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 锰硅709
    "SF": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 硅铁709
    "RS": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 油菜籽709
    "RI": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 早籼稻709
    "PM": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 普通小麦709
    "LR": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 晚籼稻709
    "JR": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 粳稻709
    "AP": [TradingPeriod(exchange_code='CZCE', period='daytime')],                                  # 苹果

    # 大商所
    "j": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 焦炭
    "i": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 铁矿石
    "jm": [TradingPeriod(exchange_code='DCE', period='daytime'),
           TradingPeriod(exchange_code='DCE', period='night')],        # 焦煤
    "a": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 黄大豆1号
    "y": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 豆油
    "m": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 豆粕
    "b": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 黄大豆2号
    "p": [TradingPeriod(exchange_code='DCE', period='daytime'),
          TradingPeriod(exchange_code='DCE', period='night')],         # 棕榈油

    "jd": [TradingPeriod(exchange_code='DCE', period='daytime')],                                   # 鲜鸡蛋1709
    "l": [TradingPeriod(exchange_code='DCE', period='daytime')],                                    # 聚乙烯1709
    "v": [TradingPeriod(exchange_code='DCE', period='daytime')],                                    # 聚氯乙烯1709
    "pp": [TradingPeriod(exchange_code='DCE', period='daytime')],                                   # 聚丙烯1709
    "fb": [TradingPeriod(exchange_code='DCE', period='daytime')],                                   # 纤维板1709
    "cs": [TradingPeriod(exchange_code='DCE', period='daytime')],                                   # 玉米淀粉1709
    "c": [TradingPeriod(exchange_code='DCE', period='daytime')],                                    # 黄玉米1709
    "bb": [TradingPeriod(exchange_code='DCE', period='daytime')],                                   # 胶合板1709

    # 上期所
    "ag": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_01')],  # 白银1709
    "au": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_01')],  # 黄金1710

    "pb": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_02')],  # 铅1709
    "ni": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_02')],  # 镍1709
    "zn": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_02')],  # 锌1709
    "al": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_02')],  # 铝1709
    "sn": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_02')],  # 锡1709
    "cu": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_02')],  # 铜1709

    "ru": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_03')],  # 天然橡胶1709
    "rb": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_03')],  # 螺纹钢1709
    "hc": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_03')],  # 热轧板1709
    "bu": [TradingPeriod(exchange_code='SHFE', period='daytime'),
           TradingPeriod(exchange_code='SHFE', period='night_group_03')],  # 沥青1809

    "wr": [TradingPeriod(exchange_code='SHFE', period='daytime')],                                       # 线材1709
    "fu": [TradingPeriod(exchange_code='SHFE', period='daytime')],                                       # 燃料油1709

    # 能源所
    "sc": [TradingPeriod(exchange_code='INE', period='daytime'),
           TradingPeriod(exchange_code='INE', period='night')]            # 螺纹钢1709
}


class TradingPeriod(object):

    @staticmethod
    def get_workdays(begin=None, end=None):
        _workdays = list()
        the_day_ts = int(time.mktime(time.strptime(begin, '%Y-%m-%d')))
        # 加 86400，表示包含 end_day 当天
        end_day_ts = int(time.mktime(time.strptime(end, '%Y-%m-%d'))) + 86400

        while the_day_ts < end_day_ts:
            the_day_structure_time = time.localtime(the_day_ts)
            the_day_ts += 86400
            the_day = time.strftime("%Y-%m-%d", the_day_structure_time)
            day_of_the_week = time.strftime("%w", the_day_structure_time)

            # 公共假日、周末 略过
            if the_day in HOLIDAYS or day_of_the_week in ['0', '6']:
                continue

            _workdays.append(the_day)

        return _workdays

    @staticmethod
    def get_exchange_trading_period_by_ts(exchange_trading_period=None, the_day=None,
                                          exchange_trading_period_by_ts=None):

        assert isinstance(exchange_trading_period, dict)

        if exchange_trading_period_by_ts is None:
            exchange_trading_period_by_ts = dict()

        assert isinstance(exchange_trading_period_by_ts, dict)
        next_day_exchange_trading_period_by_ts = None

        for k, v in exchange_trading_period.items():
            if k not in exchange_trading_period_by_ts:
                exchange_trading_period_by_ts[k] = dict()

            assert isinstance(exchange_trading_period[k], dict)

            for k2, v2 in exchange_trading_period[k].items():
                if k2 not in exchange_trading_period_by_ts[k]:
                    exchange_trading_period_by_ts[k][k2] = list()

                assert isinstance(v2, list)
                assert isinstance(exchange_trading_period_by_ts[k][k2], list)

                for item in v2:
                    ts1 = int(time.mktime(time.strptime(' '.join([the_day, item[0]]), '%Y-%m-%d %H:%M:%S')))
                    ts2 = int(time.mktime(time.strptime(' '.join([the_day, item[1]]), '%Y-%m-%d %H:%M:%S')))

                    # 交易时间跨天问题
                    if ts1 > ts2:
                        ts2 += 86400
                        # 因为时区问题，故而加8小时的秒数进行计时
                        end_of_day_ts = ts2 - (ts2 + 28800) % 86400
                        exchange_trading_period_by_ts[k][k2].append((ts1, end_of_day_ts))

                        next_day = time.strftime("%Y-%m-%d", time.localtime(ts2))

                        if not isinstance(next_day_exchange_trading_period_by_ts, dict):
                            next_day_exchange_trading_period_by_ts = dict()

                        if next_day not in next_day_exchange_trading_period_by_ts:
                            next_day_exchange_trading_period_by_ts[next_day] = dict()

                        if k not in next_day_exchange_trading_period_by_ts[next_day]:
                            next_day_exchange_trading_period_by_ts[next_day][k] = dict()

                        if k2 not in next_day_exchange_trading_period_by_ts[next_day][k]:
                            next_day_exchange_trading_period_by_ts[next_day][k][k2] = list()

                        next_day_exchange_trading_period_by_ts[next_day][k][k2].append((end_of_day_ts, ts2))

                    else:
                        exchange_trading_period_by_ts[k][k2].append((ts1, ts2))

        return exchange_trading_period_by_ts, next_day_exchange_trading_period_by_ts

    @classmethod
    def get_workdays_exchange_trading_period(cls, _workdays=None, exchange_trading_period=None):
        _workdays_exchange_trading_period_by_ts = dict()
        _next_day_exchange_trading_period_by_ts = None

        for the_day in _workdays:
            _workdays_exchange_trading_period_by_ts[the_day], _next_day_exchange_trading_period_by_ts = \
                cls.get_exchange_trading_period_by_ts(
                    exchange_trading_period=exchange_trading_period, the_day=the_day,
                    exchange_trading_period_by_ts=_next_day_exchange_trading_period_by_ts)

            if _next_day_exchange_trading_period_by_ts is not None and \
                    isinstance(_next_day_exchange_trading_period_by_ts, dict):
                for k, v in _next_day_exchange_trading_period_by_ts.items():
                    _workdays_exchange_trading_period_by_ts[k] = v
                    _next_day_exchange_trading_period_by_ts = v

        return _workdays_exchange_trading_period_by_ts


"""

workdays = TradingPeriod.get_workdays(begin='2018-1-1', end='2019-1-1')


workdays_exchange_trading_period_by_ts = \
    TradingPeriod.get_workdays_exchange_trading_period(
        _workdays=workdays, exchange_trading_period=EXCHANGE_TRADING_PERIOD)

print workdays_exchange_trading_period_by_ts

"""
