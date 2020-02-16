# coding:utf-8
'''
Authon: Tianhm
@date : 2020/2/13 下午9:46
@file : getInstrumentsFromJsonFile.py
'''

from ctp15 import TraderApi, ApiStruct
import shelve
import _pickle as pickle
import json



def get_live_instruments():
    with open('instruments_all_live.json') as f:
        instdb = json.load(f, encoding='gbk')

    numOfInst = 0
    live_insts = []
    for j in instdb:
        # numOfInst += 1
        # instj = pickle.loads(instdb[j])
        # print(j, instj)
        # print(instj.InstrumentID)
        # print(instj.InstrumentName.decode('gbk'))
        # print('now:', numOfInst)
        live_insts.append(j)

    return live_insts


def get_inst_info(instID):
    with open('instruments_all_live.json') as f:
        instdb = json.load(f, encoding='gbk')
    instinfo = instdb[instID]
    if instinfo:
        return instinfo
    else:
        return None

def get_inst_opendate(instID):
    instdb = shelve.open('instruments.slv')
    instinfo = instdb[instID]
    if instinfo:
        instinfo = pickle.loads(instinfo)
        inst_opendate = instinfo.OpenDate
    else:
        return None

    instdb.close()
    return inst_opendate


if __name__ == '__main__':
    from datetime import date, datetime, time, timedelta
    from dict_to_obj import dict_to_object
    inst_info = get_live_instruments()
    rb2005 = dict_to_object(get_inst_info('rb2005'))
    print(rb2005.OpenDate, rb2005.PriceTick, rb2005.ExpireDate, rb2005.VolumeMultiple, rb2005.ExchangeID)

    rb2005opendate=rb2005['OpenDate']

    print(rb2005['OpenDate'], rb2005['ExchangeID'], rb2005['PriceTick'], rb2005['VolumeMultiple'])
    # print(date(rb2005opendate))

    live_instruments = get_live_instruments()
    print('get instruments done.')
