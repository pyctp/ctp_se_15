pyctp
=====

ctp wrapper for python (期货版)

CTP版本：MdApi().GetApiVersion(), TraderApi().GetApiVersion()

环境：python2.5 ~ python3.4，Windows或者Linux

编译：`python setup.py build`

安装：`python setup.py install`或者复制build下的ctp目录到某个sys.path目录。
# ctp_se_15

行情服务器测试程序：

from ctp15 import ApiStruct, MdApi
import time
import traceback
import copy


ticks = []

class MyMdApi(MdApi):
    def __init__(self, instruments, broker_id,
                 investor_id, passwd, *args, **kwargs):
        self.requestid = 0
        self.instruments = instruments
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.passwd = passwd

    def OnRspError(self, info, RequestId, IsLast):
        print
        " Error"
        self.isErrorRspInfo(info)

    def isErrorRspInfo(self, info):
        if info.ErrorID != 0:
            print
            "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg
        return info.ErrorID != 0

    def OnFrontDisConnected(self, reason):
        print
        "onFrontDisConnected:", reason

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

    # def OnRspSubMarketData(self, spec_instrument, info, requestid, islast):
    #    print "OnRspSubMarketData"

    # def OnRspUnSubMarketData(self, spec_instrument, info, requestid, islast):
    #    print "OnRspUnSubMarketData"

    def OnRtnDepthMarketData(self, depth_market_data):
        tick = copy.deepcopy(depth_market_data)
        print  tick
        ticks.append(tick)


inst = [u'rb1910', u'rb1905']


def main():
    import json

    with open(r'connect_ctp.json') as f:
        acctinfo = json.load(f)
        broker_id = acctinfo['brokerID']
        investor_id = acctinfo['userID']
        password = acctinfo['password']
        mdserver = acctinfo['mdAddress']
        tdserver = acctinfo['tdAddress']

        appID = acctinfo['appID']
        authCode = acctinfo['authCode']

    user = MyMdApi(instruments=inst,
                   broker_id=broker_id,
                   investor_id=investor_id,
                   passwd=password)
                   
    user.Create("data")
    user.RegisterFront(mdserver)  # simnow std md server
   
    user.Init()
    user.Join()
 #说明：行情服务器登录，不需要认证。交易服务器才需要
 
 
 
 
 
交易服务器登录程序：





