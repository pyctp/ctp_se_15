#coding:utf-8
from ctp15 import ApiStruct, MdApi
import time
import traceback
import copy
from queue import Queue
ticks = []
tickqueue = Queue()

class MyMdApi(MdApi):
    def __init__(self, instruments, broker_id, investor_id, passwd, *args, **kwargs):
        self.requestid = 0
        self.instruments = instruments
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.passwd = passwd


    def OnRspError(self, info, RequestId, IsLast):
        print " Error"
        self.isErrorRspInfo(info)

    def isErrorRspInfo(self, info):
        if info.ErrorID != 0:
            print "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg
        return info.ErrorID != 0

    def OnFrontDisConnected(self, reason):
        print "onFrontDisConnected:", reason

    def OnHeartBeatWarning(self, time):
        print "onHeartBeatWarning", time

    def OnFrontConnected(self):
        print "OnFrontConnected:"
        self.user_login(self.broker_id, self.investor_id, self.passwd)

    def user_login(self, broker_id, investor_id, passwd):
        req = ApiStruct.ReqUserLogin(BrokerID=broker_id, UserID=investor_id, Password=passwd)

        self.requestid += 1
        r = self.ReqUserLogin(req, self.requestid)

    def OnRspUserLogin(self, userlogin, info, rid, is_last):
        print "OnRspUserLogin", is_last, info
        if is_last and not self.isErrorRspInfo(info):
            print "get today's trading day:", repr(self.GetTradingDay())
            self.subscribe_market_data(self.instruments)

    def subscribe_market_data(self, instruments):
        self.SubscribeMarketData(instruments)

    def OnRspSubMarketData(self, spec_instrument, info, requestid, islast):
       print "OnRspSubMarketData", spec_instrument

    # def OnRspUnSubMarketData(self, spec_instrument, info, requestid, islast):
    #    print "OnRspUnSubMarketData"

    def OnRtnDepthMarketData(self, depth_market_data):
        tick = copy.deepcopy(depth_market_data)

        print tick.UpdateTime,tick.InstrumentID, tick.Volume/tick.PreOpenInterest
        print tick
        ticks.append(tick)
        tickqueue.put(tick)
        



inst = [u'rb1910', u'SR909', u'i1909']

#判断是否为工作日,工作日返回1，非工作日返回0
def workDay():
    import datetime
    workTime=['09:00:00','16:00:00']
    dayOfWeek = datetime.datetime.now().weekday()
    #dayOfWeek = datetime.today().weekday()
    beginWork=datetime.datetime.now().strftime("%Y-%m-%d")+' '+workTime[0]
    endWork=datetime.datetime.now().strftime("%Y-%m-%d")+' '+workTime[1]
    beginWorkSeconds=time.time()-time.mktime(time.strptime(beginWork, '%Y-%m-%d %H:%M:%S'))
    endWorkSeconds=time.time()-time.mktime(time.strptime(endWork, '%Y-%m-%d %H:%M:%S'))
    if (int(dayOfWeek) in range(5)) and int(beginWorkSeconds)>0 and int(endWorkSeconds)<0:
        return 1
    else:
        return 0


def main():
    import json
    import time

    from data_sewing_machine import sewing_data_to_file_and_depositary

    if workDay():
        f = open(r'ctp_simnowstd.json')
    else:
        f = open(r'ctp_simnow724.json')

    acctinfo = json.load(f)
    broker_id = acctinfo['brokerID']
    investor_id = acctinfo['userID']
    password = acctinfo['password']
    mdserver = acctinfo['mdAddress']
    tdserver = acctinfo['tdAddress']

    appID = acctinfo['appID']
    authCode = acctinfo['authCode']

    f.close()

    user = MyMdApi(instruments=inst, broker_id=broker_id, investor_id=investor_id, passwd=password)

    user.Create(r"./tmp/"+"marketflow")
    user.RegisterFront(mdserver)  # simnow std md server

    user.Init()
    time.sleep(3)
    user.subscribe_market_data(['j1909','i1909', 'IF1907', 'IC1907', 'IH1907', 'MA909','TA909','ni1908'])
    time.sleep(4)
    
    while True:
        if not tickqueue.empty():
            tick = tickqueue.get()
            sewing_data_to_file_and_depositary(tick)

        time.sleep(0.5)
    # user.Join()


if __name__ == '__main__':
    main()


    # DepthMarketData(TradingDay='20190718', InstrumentID='ni1908', ExchangeID='', ExchangeInstID='', LastPrice=116660.0, PreSettlementPrice=110260.0, PreClosePrice=111040.0, PreOpenInterest=128814.0, OpenPrice=111830.0, HighestPrice=116870.0, LowestPrice=110770.0, Volume=1081920, Turnover=122466485240.0, OpenInterest=133084.0, ClosePrice=1.7976931348623157e+308, SettlementPrice=1.7976931348623157e+308, UpperLimitPrice=116870.0, LowerLimitPrice=103640.0, PreDelta=0.0, CurrDelta=1.7976931348623157e+308, UpdateTime='11:30:01', UpdateMillisec=0, BidPrice1=116570.0, BidVolume1=1, AskPrice1=116620.0, AskVolume1=6, BidPrice2=1.7976931348623157e+308, BidVolume2=0, AskPrice2=1.7976931348623157e+308, AskVolume2=0, BidPrice3=1.7976931348623157e+308, BidVolume3=0, AskPrice3=1.7976931348623157e+308, AskVolume3=0, BidPrice4=1.7976931348623157e+308, BidVolume4=0, AskPrice4=1.7976931348623157e+308, AskVolume4=0, BidPrice5=1.7976931348623157e+308, BidVolume5=0, AskPrice5=1.7976931348623157e+308, AskVolume5=0, AveragePrice=113193.66056640047, ActionDay='20190718')


