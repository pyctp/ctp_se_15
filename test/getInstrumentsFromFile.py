# coding:utf-8
'''
Authon: Tianhm
@date : 2020/2/13 下午9:46
@file : getInstrumentsFromFile.py
'''

from ctp15 import TraderApi, ApiStruct
import shelve
import cPickle as pickle



def get_live_instruments():
    instdb = shelve.open('instruments.slv')
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

    instdb.close()
    return live_insts


def get_inst_info(instID):
    instdb = shelve.open('instruments.slv')
    instinfo = instdb[instID]
    if instinfo:
        return pickle.loads(instinfo)
    else:
        return None
    instdb.close()

if __name__ == '__main__':

    rb2005 = get_inst_info('rb2005')
    print(rb2005.OpenDate, rb2005.ExchangeID, rb2005.PriceTick, rb2005.VolumeMultiple)

    live_instruments = get_live_instruments()
    print('get instruments done.')
