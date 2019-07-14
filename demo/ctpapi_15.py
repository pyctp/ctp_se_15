# encoding:utf-8
import sys
import copy
from copy import deepcopy
import time, datetime
from tnlog import Logger
import threading
import Queue
from ctp15 import ApiStruct, TraderApi, MdApi


# 加载事件驱动引擎
from event import EventEngine2, Event, EVENT_TIMER
from event.vtEvent import *


mutex = threading.Lock()
q_depth_market_data = Queue.Queue()
q_rtn_trade = Queue.Queue()
q_rtn_order = Queue.Queue()
q_positions = Queue.Queue()
q_server_info = Queue.Queue()

info = []


class MyMdApi(MdApi):
    def __init__(self, instruments, broker_id,
                 investor_id, password, eventEngine, *args, **kwargs):
        self.request_id = 0
        self.instruments = instruments
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password

        self.eventEngine = eventEngine
        self.eventEngine.start()
        self.logger = Logger()

    def OnRspError(self, info, request_id, is_last):
        print " Error: " + info

    @staticmethod
    def is_error_rsp_info(info):
        if info.ErrorID != 0:
            print "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg
        return info.ErrorID != 0

    def OnHeartBeatWarning(self, _time):
        print "onHeartBeatWarning", _time

    def OnFrontConnected(self):
        print u"connecting to the marketdata server ..."
        self.user_login(self.broker_id, self.investor_id, self.password)

    def user_login(self, broker_id, investor_id, password):
        req = ApiStruct.ReqUserLogin(BrokerID=broker_id, UserID=investor_id, Password=password)

        self.request_id += 1
        ret = self.ReqUserLogin(req, self.request_id)

    def OnRspUserLogin(self, user_login, info, rid, is_last):
        print "OnRspUserLogin", is_last
        print info

        if is_last and not self.is_error_rsp_info(info):
            # print datetime.datetime.now(), u'行情服务器登录成功。订阅合约', self.instruments
            # print "get today's trading day:", repr(self.GetTradingDay())
            self.subscribe_market_data(self.instruments)

    def OnFrontDisconnected(self, nReason):
        """当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。
        @param nReason 错误原因
                0x1001 网络读失败
                0x1002 网络写失败
                0x2001 接收心跳超时
                0x2002 发送心跳失败
                0x2003 收到错误报文
        """
        print datetime.datetime.now(), 'Marketdata server connect interrupted...', nReason

    def subscribe_market_data(self, instruments):
        self.SubscribeMarketData(instruments)

    def OnRtnDepthMarketData(self, depth_market_data):
        if depth_market_data.Volume > 0:
            tick=copy.deepcopy(depth_market_data)

            """市场行情推送"""
            # 通用事件
            event1 = Event(type_=EVENT_TICK)
            event1.dict_['data'] = tick
            self.eventEngine.put(event1)


        # print u'行情数据发送：',tick.UpdateTime, tick.InstrumentID, tick.LastPrice

class Trader(TraderApi):

    def __init__(self, broker_id, investor_id, password, appid, authcode, productinfo, request_id=1):
        self.request_id = request_id
        self.broker_id = broker_id.encode()
        self.investor_id = investor_id.encode()
        self.password = password.encode()
        self.appid = appid.encode()
        self.authcode = authcode.encode()
        self.productinfo = productinfo
        self.q_order = Queue.Queue()
        self.logger = Logger()

    @staticmethod
    def is_error_rsp_info(info):
        # print info
        if info.ErrorID != 0:
            print "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg.decode('gbk')
        return info.ErrorID != 0

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):

        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id):
        """
        :param info:
        :return:
        """
        if info.ErrorID != 0:
            print('request_id=%s ErrorID=%d, ErrorMsg=%s',
                  request_id, info.ErrorID, info.ErrorMsg.decode('gbk'))
        return info.ErrorID != 0

    def OnHeartBeatWarning(self, nTimeLapse):
        """心跳超时警告。当长时间未收到报文时，该方法被调用。
        @param nTimeLapse 距离上次接收报文的时间
        """
        print("on OnHeartBeatWarning time: ", nTimeLapse)

    def OnFrontDisconnected(self, nReason):
        print("on FrontDisConnected disconnected", nReason)

    def OnFrontConnected(self):
        print('Front connected....')
        req = ApiStruct.ReqAuthenticate(BrokerID=self.broker_id,
                                             UserID=self.investor_id,
                                        UserProductInfo = "client_vnpy",
                                             # Password=self.password,
                                                AppID=self.appid,
                                                AuthCode=self.authcode)

        r=self.ReqAuthenticate(req, self.inc_request_id())
        print(r)
        print('Req Authenticate finished...')
        time.sleep(3)

        # req = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
        #                                      UserID=self.investor_id,
        #                                      Password=self.password)
        # self.ReqUserLogin(req, self.request_id)
        # print("trader on front connection")
    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
        # print(pRspAuthenticateField.AppType)


        info.append(copy.deepcopy(pRspAuthenticateField))
        print('client authenticate done. Req User login....')
        time.sleep(3)
        req = ApiStruct.ReqUserLogin(BrokerID=self.broker_id,
                                             UserID=self.investor_id,
                                             Password=self.password)
        r=self.ReqUserLogin(req, self.inc_request_id())
        print(r)



    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):

        print('系统登录完成。user login done.')
        print datetime.datetime.now(), u"登录交易服务器...", bIsLast, pRspInfo.ErrorMsg.decode('gbk')
        print datetime.datetime.now(), 'user_login:', pRspUserLogin
        print datetime.datetime.now(), info
        print datetime.datetime.now(), nRequestID

        q_server_info.put(copy.deepcopy(pRspInfo))

        # print pRspUserLogin

        if bIsLast and not self.is_error_rsp_info(pRspInfo):

            print datetime.datetime.now(), u'交易服务器登录成功。', pRspUserLogin.BrokerID, pRspUserLogin.UserID

            # Logger.info('OnRspUserLogin %s' % is_last)
            req = ApiStruct.SettlementInfoConfirm(BrokerID=self.broker_id, InvestorID=self.investor_id)
            self.ReqSettlementInfoConfirm(req, self.inc_request_id())
            print datetime.datetime.now(), 'SettlementInfo Confirm Requested.'
            print datetime.datetime.now(), "get today's trading day:", repr(self.GetTradingDay())

        else:
            print('something wrong, connect failed....')



        #
        #
        # if pRspInfo.ErrorID != 0:
        #     print("server OnRspUserLogin failed error_id=%s msg:%s",
        #           pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk'))
        # else:
        #     print("Trader user login successfully")
        #
        #     inv = ApiStruct.QryInvestor(BrokerID=self.broker_id, InvestorID=self.investor_id)
        #
        #     self.ReqQryInvestor(inv, self.inc_request_id())
        #
        #     req = ApiStruct.SettlementInfoConfirm(BrokerID = self.broker_id, InvestorID = self.investor_id)
        #
        #     self.ReqSettlementInfoConfirm(req, self.inc_request_id())
        #     time.sleep(1)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        print(pSettlementInfoConfirm, pRspInfo)
        """投资者结算结果确认响应"""
        print datetime.datetime.now(), 'pSettlementInfoConfirm: ', pSettlementInfoConfirm
        print datetime.datetime.now(), 'pRspInfo: ', pRspInfo, pRspInfo.ErrorMsg.decode('gbk')
        print datetime.datetime.now(), 'nRequestID: ', nRequestID
        print datetime.datetime.now(), 'bIsLast: ', bIsLast
        # info.append(pSettlementInfoConfirm)
        print(pRspInfo.ErrorMsg.decode("GBK"))

    def inc_request_id(self):
        self.request_id += 1
        return self.request_id

    def OnRspQryInvestor(self, pInvestor, pRspInfo, nRequestID, bIsLast):
        print(pInvestor, pRspInfo)
        print pInvestor.InvestorName.decode('gbk'), pInvestor.Address.decode('gbk')

    def OnRtnOrder(self, pOrder):
        ''' 报单通知
            CTP、交易所接受报单
            Agent中不区分，所得信息只用于撤单
        '''

        q_rtn_order.put(deepcopy(pOrder))
        self.q_order.put(deepcopy(pOrder))

        # print repr(pOrder)

        self.logger.info(u'报单响应,Order=%s' % str(pOrder))
        # print pOrder
        if pOrder.OrderStatus == 'a':
            # CTP接受，但未发到交易所
            # print u'CTP接受Order，但未发到交易所, BrokerID=%s,BrokerOrderSeq = %s,TraderID=%s, OrderLocalID=%s' % (pOrder.BrokerID, pOrder.BrokerOrderSeq, pOrder.TraderID, pOrder.OrderLocalID)
            self.logger.info(u'TD:CTP接受Order，但未发到交易所, BrokerID=%s,BrokerOrderSeq = %s,TraderID=%s, OrderLocalID=%s' % (
                pOrder.BrokerID, pOrder.BrokerOrderSeq, pOrder.TraderID, pOrder.OrderLocalID))
            # self.agent.rtn_order(pOrder)
        else:
            # print u'交易所接受Order,exchangeID=%s,OrderSysID=%s,TraderID=%s, OrderLocalID=%s' % (pOrder.ExchangeID, pOrder.OrderSysID, pOrder.TraderID, pOrder.OrderLocalID)
            self.logger.info(u'TD:交易所接受Order,exchangeID=%s,OrderSysID=%s,TraderID=%s, OrderLocalID=%s' % (
                pOrder.ExchangeID, pOrder.OrderSysID, pOrder.TraderID, pOrder.OrderLocalID))
            # self.agent.rtn_order_exchange(pOrder)
            # self.agent.rtn_order(pOrder)

    def OnRtnTrade(self, pTrade):
        '''成交通知'''
        self.logger.info(
            u'TD:成交通知,BrokerID=%s,BrokerOrderSeq = %s,exchangeID=%s,OrderSysID=%s,TraderID=%s, OrderLocalID=%s' % (
                pTrade.BrokerID, pTrade.BrokerOrderSeq, pTrade.ExchangeID, pTrade.OrderSysID, pTrade.TraderID,
                pTrade.OrderLocalID))
        self.logger.info(u'TD:成交回报,Trade=%s' % repr(pTrade))
        pt = deepcopy(pTrade)
        # print u'报单成交：', pt.UserID, pt.InstrumentID, pt.Direction, pt.OffsetFlag, pt.Price
        q_rtn_trade.put(pt)

        # self.agent.rtn_trade(pTrade)

    def sendOrder(traderSpi, order):
        global mutex
        mutex.acquire()
        # print order

        traderSpi.ReqOrderInsert(order, traderSpi.inc_request_id())
        # DatabaseController.insert_SendOrder(order)

        # print("sendOrder = " + order.InstrumentID + " dir = " + order.Direction)
        # + " strategy = " + self.__module__)
        # time.sleep(1)
        mutex.release()

    def formatOrder(self, inst, direc, open_close, volume, price):

        orderp = ApiStruct.InputOrder(
            InstrumentID=inst,
            Direction=direc,  # ApiStruct.D_Buy or ApiStruct.D_Sell
            OrderRef=str(self.inc_request_id()),
            LimitPrice=price,
            VolumeTotalOriginal=volume,
            OrderPriceType=ApiStruct.OPT_LimitPrice,
            BrokerID=self.broker_id,
            InvestorID=self.investor_id,
            UserID=self.investor_id,
            CombOffsetFlag=open_close,  # OF_Open, OF_Close, OF_CloseToday
            CombHedgeFlag=ApiStruct.HF_Speculation,
            VolumeCondition=ApiStruct.VC_AV,
            MinVolume=1,
            ForceCloseReason=ApiStruct.FCC_NotForceClose,
            IsAutoSuspend=1,
            UserForceClose=0,
            TimeCondition=ApiStruct.TC_GFD
        )
        # print orderp
        return orderp

    def PrepareOrder(self, inst, direc, open_close, volume, price):
        order = self.formatOrder(inst, direc, open_close, volume, price)
        # print u'send order:', inst, u'price: ', price, u'amount:', volume
        self.sendOrder(order)

    def qryPosition(self, InstrumentID=''):

        self.requestid += 1

        if InstrumentID:
            req = ApiStruct.QryInvestorPosition(BrokerID=self.broker_id, InvestorID=self.investor_id,
                                                InstrumentID=InstrumentID)
        else:
            req = ApiStruct.QryInvestorPosition(BrokerID=self.broker_id, InvestorID=self.investor_id)

        self.ReqQryInvestorPosition(req, self.requestid)

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        # print pInvestorPosition
        if pInvestorPosition is not None:
            q_positions.put(copy.deepcopy(pInvestorPosition))

        # if bIsLast:
        # print pInvestorPosition,'tete'
        # print pRspInfo

    def fetch_investor_position_detail(self, instrument_id):
        '''
            获取合约的当前持仓明细，目前没用
        '''
        # logging.info(u'A:获取合约%s的当前持仓..' % (instrument_id,))
        print
        'get posi'
        req = ApiStruct.QryInvestorPositionDetail(BrokerID=self.broker_id, InvestorID=self.investor_id,
                                                  InstrumentID=instrument_id)
        self.ReqQryInvestorPositionDetail(req, self.inc_request_id())
        # logging.info(u'A:查询持仓, 函数发出返回值:%s' % r)

    def fetch_instrument_marginrate(self, instrument_id):
        req = ApiStruct.QryInstrumentMarginRate(BrokerID=self.cuser.broker_id,
                                                InvestorID=self.cuser.investor_id,
                                                InstrumentID=instrument_id,
                                                HedgeFlag=ApiStruct.HF_Speculation
                                                )
        r = self.trader.ReqQryInstrumentMarginRate(req, self.inc_request_id())
        logging.info(u'A:查询保证金率, 函数发出返回值:%s' % r)

    def fetch_instrument(self, instrument_id):
        req = ApiStruct.QryInstrument(
            InstrumentID=instrument_id,
        )
        time.sleep(1)
        r = self.trader.ReqQryInstrument(req, self.inc_request_id())
        logging.info(u'A:查询合约, 函数发出返回值:%s' % r)

    def rsp_qry_position(self, position):
        '''
            查询持仓回报, 每个合约最多得到4个持仓回报，历史多/空、今日多/空
        '''
        logging.info(u'agent 持仓:%s' % str(position))
        if position != None:
            cur_position = self.instruments[position.InstrumentID].position
            if position.PosiDirection == ApiStruct.PD_Long:
                if position.PositionDate == ApiStruct.PSD_Today:
                    cur_position.clong = position.Position  # TodayPosition
                else:
                    cur_position.hlong = position.Position  # YdPosition
            else:  # 空头
                if position.PositionDate == ApiStruct.PSD_Today:
                    cur_position.cshort = position.Position  # TodayPosition
                else:
                    cur_position.hshort = position.Position  # YdPosition
        else:  # 无持仓信息，保持默认设置
            pass
        self.check_qry_commands()

    def rsp_qry_instrument_marginrate(self, marginRate):
        '''
            查询保证金率回报.
        '''
        self.instruments[marginRate.InstrumentID].marginrate = (
            marginRate.LongMarginRatioByMoney, marginRate.ShortMarginRatioByMoney)
        # print marginRate.InstrumentID,self.instruments[marginRate.InstrumentID].marginrate
        self.check_qry_commands()

    def rsp_qry_trading_account(self, account):
        '''
            查询资金帐户回报
        '''
        self.available = account.Available
        self.check_qry_commands()

    def rsp_qry_instrument(self, pinstrument):
        '''
            获得合约数量乘数.
            这里的保证金率应该是和期货公司无关，所以不能使用
        '''
        if pinstrument.InstrumentID not in self.instruments:
            logging.warning(u'A_RQI:收到未监控的合约查询:%s' % (pinstrument.InstrumentID))
            return
        self.instruments[pinstrument.InstrumentID].multiple = pinstrument.VolumeMultiple
        self.instruments[pinstrument.InstrumentID].tick_base = int(pinstrument.PriceTick * 10 + 0.1)
        # print 'tick_base = %s' % (pinstrument.PriceTick,)
        self.check_qry_commands()

    def rsp_qry_position_detail(self, position_detail):
        '''
            查询持仓明细回报, 得到每一次成交的持仓,其中若已经平仓,则持量为0,平仓量>=1
            必须忽略
        '''
        print
        str(position_detail)
        self.check_qry_commands()

    def rsp_qry_order(self, sorder):
        '''
            查询报单
            可以忽略
        '''
        self.check_qry_commands()

    def rsp_qry_trade(self, strade):
        '''
            查询成交
            可以忽略
        '''
        self.check_qry_commands()


def main():
    import json

    with open(r'/home/tianhm/ctplrn/demo/connect_ctp_chuangyuan_thd.json') as f:
        acctinfo = json.load(f)
        broker_id = acctinfo['brokerID']
        investor_id = acctinfo['userID']
        password = acctinfo['password']
        mdsever = acctinfo['mdAddress']
        tdserver = acctinfo['tdAddress']
        appID = acctinfo['appID']
        authCode = acctinfo['authCode']
        productinfo = acctinfo['productinfo']






    td = Trader(broker_id=broker_id, investor_id=investor_id, password=password, appid=appID, authcode=authCode, productinfo = productinfo)

    td.Create('data_p')
    td.RegisterFront(tdserver)
    td.SubscribePrivateTopic(2) # 只传送登录后的流内容
    td.SubscribePrivateTopic(2) # 只传送登录后的流内容

    td.Init()

    print("trader started")
    print(td.GetTradingDay())




    while True:
        if info:
            for i in info:
                print(i)

        time.sleep(5)
    td.Join()

    # else:
    #     print("trader server down")


if __name__ == "__main__":
    main()
