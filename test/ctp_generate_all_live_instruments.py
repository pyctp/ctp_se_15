#coding:utf-8
from ctp15 import TraderApi, ApiStruct

import Queue
import time
import copy
import datetime
info = []
q_server_info = Queue.Queue()
q_positions = Queue.Queue()
q_instruments = Queue.Queue()
instruments = []

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
                                        UserProductInfo =self.productinfo,
                                             # Password=self.password,
                                                AppID=self.appid,
                                                AuthCode=self.authcode)

        r=self.ReqAuthenticate(req, self.inc_request_id())
        print(r)
        print('Req Authenticate finished..申请客户端认证提交完成.')
        time.sleep(3)

        # req = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
        #                                      UserID=self.investor_id,
        #                                      Password=self.password)
        # self.ReqUserLogin(req, self.request_id)
        # print("trader on front connection")
    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
        # print(pRspAuthenticateField.AppType)


        info.append(copy.deepcopy(pRspAuthenticateField))
        print('client authenticate done. Req User login...客户端认证完成，请求用户登录.')
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
            print datetime.datetime.now(), u'请求确认结算单。'
            req = ApiStruct.SettlementInfoConfirm(BrokerID=self.broker_id, InvestorID=self.investor_id)
            self.ReqSettlementInfoConfirm(req, self.inc_request_id())
            print datetime.datetime.now(), 'SettlementInfo Confirm Requested，结算单确认申请已提交.'
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
        info.append(pSettlementInfoConfirm)
        print u"结算单确认结果,", pRspInfo.ErrorMsg.decode("GBK")

    def inc_request_id(self):
        self.request_id += 1
        return self.request_id


    def QrySettlementInfo(self):
        """请求查询投资者结算结果"""
        req = ApiStruct.QrySettlementInfo()
        req.BrokerID = self.broker_id
        req.InvestorID = self.investor_id
        req.TradingDay = self.GetTradingDay()
        p = self.ReqQrySettlementInfo(req, self.request_id)
        print(p)

        # return 0
    def OnRspQrySettlementInfo(self, pSettlementInfo, pRspInfo, nRequestID, bIsLast):
        """请求查询投资者结算结果响应"""
        print(u'请求查询投资者结算结果响应...')
        if bIsLast:
            print(pSettlementInfo)


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

        self.request_id += 1

        if InstrumentID:
            req = ApiStruct.QryInvestorPosition(BrokerID=self.broker_id, InvestorID=self.investor_id,
                                                InstrumentID=InstrumentID)
        else:
            req = ApiStruct.QryInvestorPosition(BrokerID=self.broker_id, InvestorID=self.investor_id)

        self.ReqQryInvestorPosition(req, self.request_id)

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

    def fetch_all_instruments(self):
        # req = ApiStruct.QryInstrument(InstrumentID=instrument_id)
        req = ApiStruct.QryInstrument()
        time.sleep(1)
        r = self.ReqQryInstrument(req, self.inc_request_id())
        print(u'A:查询合约, 函数发出返回值:%s' % r)

    def fetch_instrument(self, instrument_id):
        req = ApiStruct.QryInstrument(InstrumentID=instrument_id)
        # req = ApiStruct.QryInstrument()
        time.sleep(1)
        r = self.ReqQryInstrument(req, self.inc_request_id())
        print(u'A:查询合约, 函数发出返回值:%s' % r)

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
        print(pinstrument)
        # print 'tick_base = %s' % (pinstrument.PriceTick,)
        self.check_qry_commands()

    def rsp_qry_position_detail(self, position_detail):
        '''
            查询持仓明细回报, 得到每一次成交的持仓,其中若已经平仓,则持量为0,平仓量>=1
            必须忽略
        '''
        print str(position_detail)
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
    # def ReqQryInstrument(self, pQryInstrument, nRequestID):
    #     req=ApiStruct.QryInstrument()
    #     self.ReqQryInstrument(req, self.inc_request_id())
    #     print('query instrument.')
    #
    #     return 0

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        # if bIsLast:
        instruments.append(copy.deepcopy(pInstrument))
        q_instruments.put(copy.deepcopy(pInstrument))
        # print(pInstrument)
        print(pInstrument.InstrumentName.decode('gbk'))
        print(pInstrument.InstrumentID)
        if bIsLast:
            print('done...')
def generate_all_live_instruments():

    import cPickle as pickle
    import shelve
    import json
    from ctpmdtest import workDay

    if workDay():
        f = open(r'ctp_simnowstd.json')
    else:
        f = open(r'ctp_simnow724.json')

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

    td.Create(r'./tmp/'+'traderflow')
    td.RegisterFront(tdserver)
    td.SubscribePrivateTopic(2) # 只传送登录后的流内容
    td.SubscribePrivateTopic(2) # 只传送登录后的流内容

    td.Init()
    time.sleep(10)

    print('当前交易日:'+td.GetTradingDay())

    while True:

        td.fetch_all_instruments()
        print(len(instruments))
        time.sleep(5)
        instdb = shelve.open('instruments.slv')
        while not q_instruments.empty():
            inst = q_instruments.get()
            print(inst)
            j = pickle.dumps(inst)
            instdb[inst.InstrumentID] = j


            # instfile.write(str(inst)+'\n')

            print(inst.InstrumentName.decode('gbk'))
            print(inst.OptionsType.decode('gbk'))


            '''
            Instrument(InstrumentID='i2101', ExchangeID='DCE', InstrumentName='\xcc\xfa\xbf\xf3\xca\xaf2101',
                       ExchangeInstID='i2101', ProductID='i', ProductClass='1', DeliveryYear=2021, DeliveryMonth=1,
                       MaxMarketOrderVolume=1000, MinMarketOrderVolume=1, MaxLimitOrderVolume=1000,
                       MinLimitOrderVolume=1, VolumeMultiple=100, PriceTick=0.5, CreateDate='20191216',
                       OpenDate='20200116', ExpireDate='20210115', StartDelivDate='20210118', EndDelivDate='20210120',
                       InstLifePhase='1', IsTrading=1, PositionType='2', PositionDateType='2', LongMarginRatio=0.05,
                       ShortMarginRatio=0.05, MaxMarginSideAlgorithm='0', UnderlyingInstrID='', StrikePrice=0.0,
                       OptionsType='\x00', UnderlyingMultiple=0.0, CombinationType='0')
            '''
        instdb.close()
        print('instruments saved....')
        break



if __name__ == "__main__":
    generate_all_live_instruments()

