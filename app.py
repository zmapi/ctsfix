import asyncio
import os
import argparse
import zmq
import zmq.asyncio
import json
import logging
import time
import traceback
import re
import sys
import csv
import struct
import simplefix
import ssl
import inspect
import tcache
from bidict import bidict
from sortedcontainers import SortedDict
from simplefix import FixMessage, FixParser
from enum import IntEnum
from numbers import Number
from zmapi import fix
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime
from datetime import datetime, timedelta
from copy import deepcopy
from zmapi.zmq.utils import *
from zmapi.utils import (random_str, delayed, get_timestamp, makedirs,
                         get_zmapi_dir)
from zmapi.controller import RESTConnectorCTL, ConnectorCTL
from zmapi.logging import setup_root_logger, disable_logger
from zmapi.exceptions import *
from collections import defaultdict
from uuid import uuid4


################################## CONSTANTS ##################################


CAPABILITIES = sorted([
    "SYNC_SNAPSHOT",
    # "UNSYNC_SNAPSHOT",
    "GET_TICKER_FIELDS",
    "SUBSCRIBE",
    "LIST_DIRECTORY",
    "PUB_ORDER_BOOK_INCREMENTAL",
])

TICKER_FIELDS = [
#     {
#         "field": "SecurityType",
#         "type": "selection",
#         "label": "Type",
#         "values": [
#             # only FUT and OPT are really available at the moment
#             "FUT",
#             "OPT",
#             # "STK",
#             # "SYN",
#             # "BIN"
#         ],
#         "description": "Type of the instrument"
#     },
#     {
#         "field": "SecurityExchange",
#         "type": "str",
#         "label": "Exchange",
#         "description": "Exchange of the instrument"
#     },
#     {
#         "field": "Symbol",
#         "type": "str",
#         "label": "Symbol",
#         "description": "Symbol of the instrument"
#     },
#     {
#         "field": "SecurityID",
#         "type": "str",
#         "label": "Security ID",
#         "description": "Security ID (CTS) of the instrument"
#     },
]

MODULE_NAME = "cts-fix-acmd"
ENDPOINT_NAME = "cts"


################################ GLOBAL STATE #################################


class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"
g.session_id = str(uuid4())
g.seq_no = 0

g.debug_mode = True

g.startup_event = asyncio.Event()

g.fix_seq_num_tx = 1
g.fix_seq_num_rx = None

g.cts_secid_to_insid = {}

g.sub_locks = defaultdict(asyncio.Lock)
g.sec_def_lock = asyncio.Lock()

def create_empty_subscription():
    return {
        "snapshot_buffer": [],
        "initial_snapshot_completed": False,
        "last_snap_received": 0,
        "security_trading_status": None,
        "prices": defaultdict(list),
        "tick_size": None,
    }
g.subscriptions = defaultdict(create_empty_subscription)

# placeholder for Logger
L = logging.root

# MDEntryType conversion table
CTSTICK_TO_ZMTICK = bidict()
CTSTICK_TO_ZMTICK["0"] = "0"
CTSTICK_TO_ZMTICK["1"] = "1"
CTSTICK_TO_ZMTICK["2"] = "E"
CTSTICK_TO_ZMTICK["3"] = "F"
CTSTICK_TO_ZMTICK["4"] = "2"
CTSTICK_TO_ZMTICK["6"] = "6"
CTSTICK_TO_ZMTICK["7"] = "7"
CTSTICK_TO_ZMTICK["8"] = "8"
CTSTICK_TO_ZMTICK["9"] = "4"
# CTSTICK_TO_ZMTICK["K"] = "K"
# CTSTICK_TO_ZMTICK["L"] = "L"

# TradSesStatus conversion table
CTSTS_TO_ZMTS = {}
CTSTS_TO_ZMTS[0] = fix.SecurityTradingStatus.UnknownOrInvalid
CTSTS_TO_ZMTS[1] = fix.SecurityTradingStatus.PreOpen
CTSTS_TO_ZMTS[2] = fix.SecurityTradingStatus.ReadyToTrade
CTSTS_TO_ZMTS[3] = fix.SecurityTradingStatus.RestrictedOpen
CTSTS_TO_ZMTS[4] = fix.SecurityTradingStatus.PreClose
CTSTS_TO_ZMTS[5] = fix.SecurityTradingStatus.PostClose
CTSTS_TO_ZMTS[7] = fix.SecurityTradingStatus.TradingHalt

# AggressorSide conversion table
CTSAGG_TO_ZMAGG = {}
CTSAGG_TO_ZMAGG["-1"] = fix.AggressorSide.Undisclosed
CTSAGG_TO_ZMAGG["0"] = fix.AggressorSide.Buy
CTSAGG_TO_ZMAGG["2"] = fix.AggressorSide.Sell

SEC_SUB_TYPE = bidict()
SEC_SUB_TYPE[0] = "Outright"
SEC_SUB_TYPE[1] = "CalendarSpread"
SEC_SUB_TYPE[2] = "RTCalendarSpread"
SEC_SUB_TYPE[3] = "InterContractSpread"
SEC_SUB_TYPE[4] = "Butterfly"
SEC_SUB_TYPE[5] = "Condor"
SEC_SUB_TYPE[6] = "DoubleButterfly"
SEC_SUB_TYPE[7] = "Horizontal"
SEC_SUB_TYPE[8] = "Bundle"
SEC_SUB_TYPE[9] = "MonthVsPack"
SEC_SUB_TYPE[10] = "Pack"
SEC_SUB_TYPE[11] = "PackSpread"
SEC_SUB_TYPE[12] = "PackButterfly"
SEC_SUB_TYPE[13] = "BundleSpread"
SEC_SUB_TYPE[14] = "Strip"
SEC_SUB_TYPE[15] = "Crack"
SEC_SUB_TYPE[16] = "TreasurySpread"
SEC_SUB_TYPE[17] = "Crush"
SEC_SUB_TYPE[19] = "Threeway"
SEC_SUB_TYPE[20] = "ThreewayStraddleVsCall"
SEC_SUB_TYPE[21] = "ThreewayStraddleVsPut"
SEC_SUB_TYPE[22] = "Box"
SEC_SUB_TYPE[23] = "ChristmasTree"
SEC_SUB_TYPE[24] = "ConditionalCurve"
SEC_SUB_TYPE[25] = "Double"
SEC_SUB_TYPE[26] = "HorizontalStraddle"
SEC_SUB_TYPE[27] = "IronCondor"
SEC_SUB_TYPE[28] = "Ratio1x2"
SEC_SUB_TYPE[29] = "Ratio1x3"
SEC_SUB_TYPE[30] = "Ratio2x3"
SEC_SUB_TYPE[31] = "RiskReversal"
SEC_SUB_TYPE[32] = "StraddleStrip"
SEC_SUB_TYPE[33] = "Straddle"
SEC_SUB_TYPE[34] = "Strangle"
SEC_SUB_TYPE[35] = "Vertical"
SEC_SUB_TYPE[36] = "JellyRoll"
SEC_SUB_TYPE[37] = "IronButterfly"
SEC_SUB_TYPE[38] = "Guts"
SEC_SUB_TYPE[39] = "Generic"
SEC_SUB_TYPE[40] = "Diagonal"
SEC_SUB_TYPE[41] = "CoveredThreeway"
SEC_SUB_TYPE[42] = "CoveredThreewayStraddleVsCall"
SEC_SUB_TYPE[43] = "CoveredThreewayStraddleVsPut"
SEC_SUB_TYPE[44] = "CoveredBox"
SEC_SUB_TYPE[45] = "CoveredChristmasTree"
SEC_SUB_TYPE[46] = "CoveredConditionalCurve"
SEC_SUB_TYPE[47] = "CoveredDouble"
SEC_SUB_TYPE[48] = "CoveredHorizontalStraddle"
SEC_SUB_TYPE[49] = "CoveredIronCondor"
SEC_SUB_TYPE[50] = "CoveredRatio1x2"
SEC_SUB_TYPE[51] = "CoveredRatio1x3"
SEC_SUB_TYPE[52] = "CoveredRatio2x3"
SEC_SUB_TYPE[53] = "CoveredRiskReversal"
SEC_SUB_TYPE[54] = "CoveredStraddleStrip"
SEC_SUB_TYPE[55] = "CoveredStraddle"
SEC_SUB_TYPE[56] = "CoveredStrangle"
SEC_SUB_TYPE[57] = "CoveredVertical"
SEC_SUB_TYPE[58] = "CoveredJellyRoll"
SEC_SUB_TYPE[59] = "CoveredIronButterfly"
SEC_SUB_TYPE[60] = "CoveredGuts"
SEC_SUB_TYPE[61] = "CoveredGeneric"
SEC_SUB_TYPE[62] = "CoveredDiagonal"
SEC_SUB_TYPE[63] = "CoveredButterfly"
SEC_SUB_TYPE[64] = "CoveredCondor"
SEC_SUB_TYPE[65] = "CoveredHorizontal"
SEC_SUB_TYPE[66] = "CoveredStrip"
SEC_SUB_TYPE[67] = "CoveredOption"
SEC_SUB_TYPE[68] = "BalancedStrip"
SEC_SUB_TYPE[69] = "UnbalancedStrip"
SEC_SUB_TYPE[70] = "InterContractStrip"

PUT_OR_CALL = bidict()
PUT_OR_CALL[0] = "Put"
PUT_OR_CALL[1] = "Call"

# FIX fields for CTS
class Fields:
    BeginString=8
    Currency=15
    MsgSeqNum=34
    MsgType=35
    OrdType=40
    SecurityID=48
    SenderCompID=49
    SenderSubID=50
    SendingTime=52
    Symbol=55
    TargetCompID=56
    Text=58
    SecureDataLen=90
    SecureData=91
    EncryptMethod=98
    SecurityDesc=107
    HeartBtInt=108
    TestReqID=112
    SecurityType=167
    MaturityMonthYear=200
    PutOrCall=201
    StrikePrice=202
    MaturityDay=205
    SecurityExchange=207
    SecurityTradingStatus=326
    MDReqID=262
    SubscriptionRequestType=263
    MarketDepth=264
    MDUpdateType=265
    NoMDEntryTypes=267
    NoMDEntries=268
    MDEntryType=269
    MDEntryPx=270
    MDEntrySize=271
    TickDirection=274
    MDUpdateAction=279
    MDReqRejReason=281
    SecurityReqID=320
    SecurityRequestType=321
    SecurityResponseType=323
    RefMsgType=372
    NoMsgTypes=384
    TotalVolumeTraded=387
    NoSecurityAltID=454
    SecurityAltID=455
    SecurityAltIDSource=456
    MinTradeVol=562
    UserName=553
    Password=554
    NoLegs=555
    LegCurrency=556
    LegSymbol=600
    LegSecurityID=602
    LegSecurityType=609
    LegMaturityMonthYear=610
    LegStrikePrice=612
    LegSecurityExchange=616
    LegSecurityDesc=620
    LegRatioQty=623
    LegSide=624
    SecuritySubType=762
    TotNumReports=911
    SecurityStatus=965
    MDEntryLevel=1023
    MinPriceIncrementAmount=1146
    LegPutOrCall=1358
    DefaultCstmApplVerID=1408
    PriceRatio=5770


###############################################################################


def dict_to_key(d):
    return str(SortedDict(d))


def insid_to_market(ins_id):
    spl = [x for x in ins_id.split("/") if x]
    d = {}
    d["SecurityType"] = spl[0]
    d["SecurityExchange"] = spl[1]
    d["Symbol"] = spl[2]
    if d["SecurityType"] == "OPT" and len(spl) == 5:
        d["PutOrCall"] = PUT_OR_CALL.inv[spl[3]]
        d["SecurityID"] = spl[4]
        if len(spl) > 5:
            raise ValueError("invalid ZMInstrumentID")
    else:
        d["SecurityID"] = spl[3]
        if len(spl) > 4:
            raise ValueError("invalid ZMInstrumentID")
    return d


def market_to_insid(d):
    if not d.get("SecurityType") or \
            not d.get("SecurityExchange") or \
            not d.get("Symbol") or \
            not d.get("SecurityID"):
        raise ValueError("invalid input data")
    poc = d.get("PutOrCall")
    if poc is None:
        return "/".join([
            "",
            d["SecurityType"],
            d["SecurityExchange"],
            d["Symbol"],
            d["SecurityID"],
        ])
    else:
        assert d["SecurityType"] == "OPT", d
        return "/".join([
            "",
            d["SecurityType"],
            d["SecurityExchange"],
            d["Symbol"],
            PUT_OR_CALL[poc],
            d["SecurityID"],
        ])


def conv_to_int_repr(val):
    for i in range(1000):
        x = val * (10 ** i)
        if abs(x - int(x)) < 1e-12:
            return int(x)


###############################################################################



class MDController(ConnectorCTL):


    def __init__(self, sock_dn):
        super().__init__(sock_dn, "MD")
        

    async def ZMGetStatus(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetStatusResponse
        d = {}
        d["module_name"] = MODULE_NAME
        d["endpoint_name"] = ENDPOINT_NAME
        d["session_id"] = g.session_id
        res["Body"] = [d]
        return res


    async def ZMListCapabilities(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListCapabilitiesResponse
        res["Body"] = body = {}
        body["ZMCaps"] = CAPABILITIES
        return res


    async def ZMGetInstrumentFields(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetInstrumentFieldsResponse
        res["Body"] = TICKER_FIELDS
        return res


    async def SecurityListRequest(self, ident, msg_raw, msg):

        body = msg["Body"]
        data = {}

        ins_id = body.get("ZMInstrumentID")
        if ins_id:
            try:
                data = insid_to_market(ins_id)
            except:
                raise BusinessMessageRejectException(
                        "invalid ZMInstrumentID")
        else:
            if "SecurityType" in body:
                data["SecurityType"] = body["SecurityType"]
            if "SecurityExchange" in body:
                data["SecurityExchange"] = body["SecurityExchange"]
            if "PutOrCall" in body:
                data["PutOrCall"] = body["PutOrCall"]
            if "Symbol" in body:
                data["Symbol"] = body["Symbol"]
            if "SecuritySubType" in body:
                data["SecuritySubType"] = body["SecuritySubType"]
            if "SecurityID" in body:
                data["SecurityID"] = body["SecurityID"]
            if "SecurityRequestType" in body:
                data["SecurityRequestType"] = body["SecurityRequestType"]
            if "MaturityMonthYear" in body:
                data["MaturityMonthYear"] = body["MaturityMonthYear"]

        fix_msgs = await g.fix_client.security_definition_request(data)

        if len(fix_msgs) == 1:
            if fix_msgs[0].get(Fields.TotNumReports) == b"0":
                # Some error happened
                err_msg = fix_msgs[0].gets(Fields.SecurityDesc)
                raise BusinessMessageRejectException(err_msg)

        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.SecurityList
        res["Body"] = body = {}
        body["SecListGrp"] = group = []

        for msg in fix_msgs:

            d = {}
            
            d["SecurityType"] = msg.gets(Fields.SecurityType)
            d["SecurityExchange"] = msg.gets(Fields.SecurityExchange)
            d["Symbol"] = msg.gets(Fields.Symbol)
            d["SecurityID"] = msg.gets(Fields.SecurityID)
            # TODO: standardize OrdType (when writing AC specs)
            d["MaturityMonthYear"] = msg.gets(Fields.MaturityMonthYear)
            d["MaturityDate"] = "{}{}".format(d["MaturityMonthYear"],
                                              msg.gets(Fields.MaturityDay))
            min_trade_vol = msg.get(Fields.MinTradeVol)
            if min_trade_vol:
                d["MinTradeVol"] = float(min_trade_vol)
            d["Currency"] = msg.gets(Fields.Currency)
            # TODO: when to use RTS value?
            # TODO: read and return variable tick size table
            # example PriceRatio: 3125/100000 RTS(78125/10000000)
            price_ratio = msg.gets(Fields.PriceRatio)
            spl = price_ratio.split()[0].split("/")
            num = float(spl[0])
            denom = float(spl[1])
            d["MinPriceIncrement"] = conv_to_int_repr(num / denom)
            # example MinPriceIncrementAmount: 31.25 RTS(7.8125)
            min_price_inc_amt = msg.gets(Fields.MinPriceIncrementAmount)
            d["ContractMultiplier"] = float(min_price_inc_amt.split()[0])
            d["SecurityDesc"] = msg.gets(Fields.SecurityDesc)
            d["SecuritySubType"] = msg.gets(Fields.SecuritySubType)
            strike = msg.get(Fields.StrikePrice)
            if strike:
                d["StrikePrice"] = float(strike)
            d["PutOrCall"] = msg.geti(Fields.PutOrCall)
            d = {k: v for k, v in d.items() if v is not None}

            num_legs = msg.get(Fields.NoLegs)
            if num_legs:
                d["InstrmtLegSecListGrp"] = group2 = []
                num_legs = int(num_legs)
                # 1-based indexing
                for i in range(1, num_legs + 1):
                    leg = {}
                    leg["Symbol"] = msg.gets(Fields.LegSymbol, i,
                                             Fields.LegSymbol)
                    leg["RatioQty"] = msg.gets(Fields.LegRatioQty, i,
                                               Fields.LegSymbol)
                    leg["LegSide"] = msg.gets(Fields.LegSide, i,
                                              Fields.LegSymbol)
                    leg["SecurityType"] = msg.gets(Fields.LegSecurityType, i,
                                                   Fields.LegSymbol)
                    leg["SecurityID"] = msg.gets(Fields.LegSecurityID, i,
                                                 Fields.LegSymbol)
                    leg["SecurityExchange"] = \
                            msg.gets(Fields.LegSecurityExchange, i,
                                     Fields.LegSymbol)
                    leg["SecurityDesc"] = msg.gets(Fields.LegSecurityDesc, i,
                                                   Fields.LegSymbol)
                    leg["Currency"] = msg.gets(Fields.LegCurrency, i,
                                               Fields.LegSymbol)
                    leg["MaturityMonthYear"] = \
                            msg.gets(Fields.LegMaturityMonthYear, i,
                                     Fields.LegSymbol)
                    strike = msg.get(Fields.LegStrikePrice, i,
                                     Fields.LegSymbol)
                    if strike:
                        leg["StrikePrice"] = float(strike)
                    leg["PutOrCall"] = msg.gets(Fields.LegPutOrCall, i,
                                                Fields.LegSymbol)
                    leg = {k: v for k, v in leg.items() if v is not None}
                    group2.append(leg)

            num_sec_alt_ids = msg.get(Fields.NoSecurityAltID)
            if num_sec_alt_ids:
                d["SecAltIDGrp"] = group2 = []
                num_sec_alt_ids = int(num_sec_alt_ids)
                for i in range(num_sec_alt_ids):
                    aid = {}
                    aid["SecurityAltID"] = msg.gets(Fields.SecurityAltID, i)
                    aid["SecurityAltIDSource"] = \
                            msg.gets(Fields.SecurityAltIDSource, i)
                    group2.append(aid)

            try:
                d["ZMInstrumentID"] = market_to_insid(d)
            except:
                L.debug("skipped invalid secdef:\n{}".format(pformat(d)))
                continue
            group.append(d)

        return res


    async def ZMListDirectory(self, ident, msg_raw, msg):

        body = msg["Body"]
        dir = body.get("ZMPath", "/")

        if dir == "/":
            res = {}
            res["Header"] = header = {}
            header["MsgType"] = fix.MsgType.ZMListDirectoryResponse
            res["Body"] = body = {}
            group = []
            group.append({"ZMNodeName": "FUT", "Text": "Futures"})
            group.append({"ZMNodeName": "OPT", "Text": "Options"})
            # group.append({"ZMNodeName": "STK", "Text": "Stocks"})
            # group.append({"ZMNodeName": "SYN", "Text": "Synthetic"})
            # group.append({"ZMNodeName": "BIN", "Text": "Binary options"})
            group = sorted(group, key=lambda x: x["ZMNodeName"])
            body["ZMDirEntries"] = group
            return res

        spl = [x for x in dir.split("/") if x]

        ret_sub_types = False
        ret_maturities = False

        data = {}
        if len(spl) < 1:
            raise BusinessMessageRejectException("invalid ZMPath")
        if len(spl) >= 1:
            data["SecurityType"] = spl[0]
        if len(spl) >= 2:
            data["SecurityExchange"] = spl[1]
        if len(spl) >= 3:
            data["Symbol"] = spl[2]
        if len(spl) == 3:
            ret_sub_types = True
        if data["SecurityType"] == "OPT":
            if len(spl) >= 4:
                sst = spl[3]
                data["PutOrCall"] = PUT_OR_CALL.inv.get(
                        sst, SEC_SUB_TYPE.inv.get(sst, sst))
            if len(spl) == 4:
                ret_maturities = True
            if len(spl) >= 5:
                data["MaturityMonthYear"] = spl[4]
            if len(spl) >= 6:
                data["SecurityID"] = spl[5]
            if len(spl) >= 7:
                raise BusinessMessageRejectException("invalid ZMPath")

        else:
            if len(spl) >= 4:
                data["SecuritySubType"] = SEC_SUB_TYPE.inv.get(spl[3], spl[3])
            if len(spl) >= 5:
                data["SecurityID"] = spl[4]
            if len(spl) >= 6:
                raise BusinessMessageRejectException("invalid ZMPath")

        fix_msgs = await g.fix_client.security_definition_request(data)

        if len(fix_msgs) == 1:
            if fix_msgs[0].get(Fields.TotNumReports) == b"0":
                # Some error happened
                err_msg = fix_msgs[0].gets(Fields.SecurityDesc)
                raise BusinessMessageRejectException(err_msg)

        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListDirectoryResponse
        res["Body"] = body = {}
        group = []

        if ret_maturities:
            maturities = set()
            for msg in fix_msgs:
                maturity = msg.gets(Fields.MaturityMonthYear)
                if not maturity:
                    maturity = "UnspecifiedMaturity"
                maturities.add(maturity)
            maturities = sorted(maturities)
            for maturity in maturities:
                d = {}
                d["ZMNodeName"] = d["Text"] = maturity
                group.append(d)
            body["ZMDirEntries"] = group
            return res

        if ret_sub_types:
            if data["SecurityType"] == "OPT":
                d = {}
                d["ZMNodeName"] = d["Text"] = "Call"
                group.append(d)
                d = {}
                d["ZMNodeName"] = d["Text"] = "Put"
                group.append(d)
            subtypes = set()
            for msg in fix_msgs:
                sst = int(msg.get(Fields.SecuritySubType))
                subtypes.add(sst)
            subtypes = sorted(subtypes)
            for sst in subtypes:
                d = {}
                d["ZMNodeName"] = SEC_SUB_TYPE.get(sst, str(sst))
                d["Text"] = d["ZMNodeName"]
                group.append(d)
            body["ZMDirEntries"] = group
            return res

        order = []
        for msg in fix_msgs:
            d = {}
            mkt = {}
            mkt["SecurityType"] = msg.gets(Fields.SecurityType)
            if mkt["SecurityType"] != data["SecurityType"]:
                continue
            mkt["SecurityExchange"] = msg.gets(Fields.SecurityExchange)
            mkt["Symbol"] = msg.gets(Fields.Symbol)
            mkt["SecurityID"] = msg.gets(Fields.SecurityID)
            poc = msg.get(Fields.PutOrCall)
            if poc:
                mkt["PutOrCall"] = int(poc)
            try:
                d["ZMInstrumentID"] = market_to_insid(mkt)
            except:
                pass
            if len(spl) == 1:
                d["ZMNodeName"] = msg.gets(Fields.SecurityExchange)
            elif len(spl) == 2:
                d["ZMNodeName"] = msg.gets(Fields.Symbol)
            else:
                d["ZMNodeName"] = msg.gets(Fields.SecurityID)
            d["Text"] = msg.gets(Fields.SecurityDesc)
            group.append(d)
            strike = msg.getf(Fields.StrikePrice)
            if strike is None:
                strike = 0.0
            order.append((str(msg.gets(Fields.MaturityMonthYear)),
                          strike,
                          d["ZMNodeName"]))

        group = [x for _, x in sorted(zip(order, group), key=lambda t: t[0])]
        body["ZMDirEntries"] = group

        return res


    async def MarketDataRequest(self, ident, msg_raw, msg):

        body = msg["Body"]
        ticks = body["MDReqGrp"]

        srt = body["SubscriptionRequestType"]
        if srt not in "012":
            raise MarketDataRequestRejectException(
                    fix.UnsupportedSubscriptionRequestType, srt)

        ins_id = body["ZMInstrumentID"]

        # await g.sub_locks[ins_id].acquire()

        try:
            data = insid_to_market(ins_id)
        except:
            raise MarketDataRequestRejectException("invalid ZMInstrumentID")

        g.cts_secid_to_insid[data["SecurityID"]] = ins_id
        sub = g.subscriptions[data["SecurityID"]]

        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMMarketDataRequestResponse
        res["Body"] = {}

    
        if srt == "0":
            raise BusinessMessageRejectException(
                    "unsync snapshots not implemented yet")

        if srt == "1":
            data["SubscriptionRequestType"] = "7"

            ticks = body.get("MDReqGrp")
            data["MarketDepth"] = body["MarketDepth"]
            if data["MarketDepth"] == 0:  # all levels
                data["MarketDepth"] = 10
            # TODO: implement speed adjustments
            data["MDUpdateType"] = 8  # T4 Fast Trade
            s = ""
            for zmtick in CTSTICK_TO_ZMTICK.values():
                if "*" in ticks or zmtick in ticks:
                    s += CTSTICK_TO_ZMTICK.inv[zmtick]
            data["MDReqGrp"] = s
            s = ""
            for ctstick in data["MDReqGrp"]:
                s += CTSTICK_TO_ZMTICK[ctstick]
            sub["MDReqGrp"] = s

        elif srt == "2":
            data["SubscriptionRequestType"] = "2"
            data["MarketDepth"] = 10
            data["MDReqGrp"] = "".join(sorted(CTSTICK_TO_ZMTICK.keys()))

        res["Body"]["Text"] = await g.fix_client.send_market_data_request(data)
        return res


    # TODO: write TradingSessionStatusRequest

    
###############################################################################


class FIXReader:

    PUB_ADDR = "inproc://fix-pub"

    def __init__(self, reader):
        self._reader = reader
        self._parser = FixParser()
        self._sock_pub = g.ctx.socket(zmq.PUB)
        self._sock_pub.bind(self.PUB_ADDR)
        self.connected = False


    async def read_forever(self):
        L.info("FIXReader: starting reader coroutine ...")
        while True:
            data = await self._reader.read(1024)
            if not data:
                L.critical("FIXReader: disconnected")
                self.connected = False
                break
            # L.debug("got {} bytes".format(len(data)))
            self._parser.append_buffer(data)
            while True:
                msg = self._parser.get_message()
                if not msg:
                    break
                L.debug("< {}".format(msg))
                try:
                    await self._handle_msg(msg)
                except Exception as err:
                    L.exception("error handling fix message:")


    async def _handle_msg(self, msg):
        seq_num = int(msg.get(Fields.MsgSeqNum))
        if g.fix_seq_num_rx is not None and seq_num - g.fix_seq_num_rx != 1:
            # TODO: restore connection by ResendRequest or SequenceReset
            L.critical("invalid rx seq_num: expected {}, got {}"
                       .format(g.fix_seq_num_rx, seq_num))
            sys.exit(1)
        g.fix_seq_num_rx = seq_num
        msg_type = msg.gets(Fields.MsgType)
        handler = self.HANDLERS.get(msg_type)
        if handler:
            if inspect.iscoroutinefunction(handler):
                await handler(self, msg)
            else:
                handler(self, msg)
        # some performance lost here ...
        await self._sock_pub.send_multipart([msg_type.encode(), msg.encode()])


    def handle_test_request(self, msg):
        req_id = msg.gets(Fields.TestReqID)
        L.debug("test_request received: {}".format(req_id))
        g.fix_client.send_heartbeat(req_id)
                

    def handle_logon(self, msg):
        sender = msg.gets(Fields.SenderSubID)
        user = msg.gets(Fields.UserName)
        version = msg.gets(Fields.DefaultCstmApplVerID)
        L.info("Login succesful: sender={}, user={}, version={}"
               .format(sender, user, version))
        g.startup_event.set()
        self.connected = True
        create_task(g.fix_client.pinger())


    def handle_security_definition(self, msg):
        pass
        # sec_id = msg.gets(Fields.SecurityID)
        # sub = g.subscriptions[sec_id]
        # price_ratio = msg.gets(Fields.PriceRatio)
        # spl = price_ratio.split()[0].split("/")
        # numerator = float(spl[0])
        # denominator = float(spl[1])
        # sub["tick_size"] = round(numerator / denominator)


    async def _emit_security_status(self, sec_id, sub, data):
        data = deepcopy(data)
        sts = data.get("SecurityTradingStatus")
        zmts = CTSTS_TO_ZMTS.get(sts)
        if zmts is not None:
            data["SecurityTradingStatus"] = zmts
        else:
            data.pop("SecurityTradingStatus", None)
        prev_sts = sub["security_trading_status"]
        if zmts == prev_sts:
            data.pop("SecurityTradingStatus", None)
        if sts is not None and zmts is None:
            L.warning("Unknown CTSTS on {}: {}".format(sec_id, sts))
            return
        seq_no = g.seq_no
        g.seq_no += 1
        d = {}
        d["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = get_timestamp()
        d["Body"] = body = data
        ins_id = g.cts_secid_to_insid[sec_id]
        body["ZMTickerID"] = g.ctl.insid_to_tid[ins_id]
        data = " " + json.dumps(d)
        data = [b"h", data.encode()]
        # # TODO: send only SecurityStatus messages when requested with
        # # SecurityStatus message
        # # await g.sock_pub.send_multipart(data)


    async def _flush_snap_buffer(self, sub):

        if sub["initial_snapshot_completed"]:
            return

        snaps = sub["snapshot_buffer"]
        L.debug("flushing snapshot_buffer ({} messages) ..."
                .format(len(snaps)))

        sec_id = snaps[0].gets(Fields.SecurityID)
        ins_id = g.cts_secid_to_insid[sec_id]

        sts = None
        last_trade = None  # include only latest trade
        hi_lim = None
        lo_lim = None
        for snap in snaps[::-1]:
            if sts is None:
                sts = snap.geti(Fields.SecurityStatus)
            if not last_trade:
                num_md_entries = snap.geti(Fields.NoMDEntries, default=0)
                for i in range(1, num_md_entries + 1):
                    et = snap.gets(Fields.MDEntryType, i, Fields.MDEntryType)
                    if et == "4":  # Trade
                        d = {}
                        d["MDEntryType"] = fix.MDEntryType.Trade
                        d["MDEntryPx"] = snap.getf(
                                Fields.MDEntryPx, i, Fields.MDEntryType)
                        d["MDEntrySize"] = snap.getf(
                                Fields.MDEntrySize, i, Fields.MDEntryType)
                        agg = snap.gets(Fields.TickDirection)
                        if agg is not None:
                            d["AggressorSide"] = CTSAGG_TO_ZMAGG[agg]
                        last_trade = d
                        continue
                    if et not in "KL":
                        continue
                    if et == "L":
                        lo_lim = snap.geti(
                                Fields.MDEntryPx, i, Fields.MDEntryType)
                        continue
                    if et == "K":
                        hi_lim = snap.geti(
                                Fields.MDEntryPx, i, Fields.MDEntryType)
            if sts is not None and last_trade \
                    and hi_lim is not None \
                    and lo_lim is not None:
                break
        
        ss_data = {}
        if sts is not None:
            ss_data["SecurityTradingStatus"] = sts
        else:
            L.warning("{}: SecurityTradingStatus not found in snaps"
                      .format(sec_id))
        pl = {}
        if hi_lim is not None:
            pl["HighLimitPrice"] = hi_lim
        if lo_lim is not None:
            pl["LowLimitPrice"] = lo_lim
        if pl:
            ss_data["PriceLimits"] = pl
        if ss_data:
            await self._emit_security_status(sec_id, sub, data)

        seq_no = g.seq_no
        g.seq_no += 1
        d = {}
        d["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = get_timestamp()
        d["Body"] = body = {}
        ttime = snaps[0].gets(Fields.SendingTime)
        ttime = datetime.strptime(ttime, "%Y%m%d-%H:%M:%S.%f")
        ttime = int(ttime.timestamp() * 1e9)
        body["SendingTime"] = ttime
        body["ZMTickerID"] = g.ctl.insid_to_tid[ins_id]
        body["MDFullGrp"] = group = []

        for snap in snaps:
            num_md_entries = snap.get(Fields.NoMDEntries)
            num_md_entries = int(num_md_entries) if num_md_entries else 0
            for i in range(1, num_md_entries + 1):
                entry = {}
                cts_et = snap.gets(
                        Fields.MDEntryType, i, Fields.MDEntryType)
                if cts_et in "4LK":  # Trade and Hi/Lo limits handled earlier
                    continue
                et = CTSTICK_TO_ZMTICK.get(cts_et)
                if et is None:
                    L.warning("{}: unrecognized MDEntryType on "
                              "initial snapshot '{}'".format(sec_id, cts_et))
                    continue
                entry["MDEntryType"] = et
                entry_px = snap.get(Fields.MDEntryPx, i, Fields.MDEntryType)
                if entry_px is not None:
                    entry["MDEntryPx"] = float(entry_px)
                entry_size = snap.get(Fields.MDEntrySize, i, Fields.MDEntryType)
                if entry_size is not None:
                    entry["MDEntrySize"] = float(entry_size)
                pos = snap.get(Fields.MDEntryLevel, i, Fields.MDEntryType)
                if pos is not None:
                    entry["MDPriceLevel"] = int(pos)
                group.append(entry)
        if last_trade:
            group.append(last_trade)
        else:
            L.warning("{}: last_trade not found in snaps".format(sec_id))

        sub["prices"].clear()

        lvls = [x for x in group if x["MDEntryType"] == "0"]
        lvls = sorted(lvls, key=lambda x: x["MDEntryPx"])[::-1]
        for entry in lvls:
            sub["prices"][entry["MDEntryType"]].append(int(entry["MDEntryPx"]))

        lvls = [x for x in group if x["MDEntryType"] == "E"]
        lvls = sorted(lvls, key=lambda x: x["MDEntryPx"])[::-1]
        for entry in lvls:
            sub["prices"][entry["MDEntryType"]].append(int(entry["MDEntryPx"]))

        lvls = [x for x in group if x["MDEntryType"] == "1"]
        lvls = sorted(lvls, key=lambda x: x["MDEntryPx"])
        for entry in lvls:
            sub["prices"][entry["MDEntryType"]].append(int(entry["MDEntryPx"]))

        lvls = [x for x in group if x["MDEntryType"] == "F"]
        lvls = sorted(lvls, key=lambda x: x["MDEntryPx"])
        for entry in lvls:
            sub["prices"][entry["MDEntryType"]].append(int(entry["MDEntryPx"]))

        L.debug("prices after W: {}".format(sub["prices"]))

        data = " " + json.dumps(d)
        data = [b"W", data.encode()]
        await g.sock_pub.send_multipart(data)

        sub["initial_snapshot_completed"] = True
        sub["snapshot_buffer"].clear()

        # g.sub_locks[ins_id].release()


    async def _snapshot_timeout_timer(self, sub):
        timeout = 0.1
        while True:
            elapsed = time() - sub["last_snap_received"]
            if elapsed > timeout:
                break
            await asyncio.sleep(timeout - elapsed)
        L.debug("snapshot timeout elapsed ...")
        await self._flush_snap_buffer(sub)


    async def handle_md_snap_full_refresh(self, msg):

        sec_id = msg.gets(Fields.SecurityID)
        if sec_id not in g.cts_secid_to_insid:
            L.warning("SecurityID not subscribed: {}".format(sec_id))
            return
        sub = g.subscriptions[sec_id]
        if not sub["initial_snapshot_completed"]:
            if not sub["snapshot_buffer"]:
                create_task(self._snapshot_timeout_timer(sub))
            sub["snapshot_buffer"].append(msg)
            sub["last_snap_received"] = time()
            return

        # trades and hi/lo limits are reported as W messages for some reason...
        seq_no = g.seq_no
        g.seq_no += 1
        d = {}
        d["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = get_timestamp()
        d["Body"] = body = {}
        ttime = msg.gets(Fields.SendingTime)
        ttime = datetime.strptime(ttime, "%Y%m%d-%H:%M:%S.%f")
        ttime = int(ttime.timestamp() * 1e9)
        body["SendingTime"] = ttime
        ins_id = g.cts_secid_to_insid[sec_id]
        tid = g.ctl.insid_to_tid[ins_id]
        body["MDIncGrp"] = group = []
        num_entries = int(msg.get(Fields.NoMDEntries))

        pl = {}

        for i in range(1, num_entries + 1):
            et = msg.gets(Fields.MDEntryType, i, Fields.MDEntryType)
            if et not in "4KL":
                L.warning("{}: unexpected MDEntryType on W update '{}'"
                          .format(sec_id, et))
                L.warning("msg: {}".format(msg))
                continue
            if et == "K":
                pl["HighLimitPrice"] = snap.getf(
                        Fields.MDEntryPx, i, Fields.MDEntryType)
                continue
            if et == "L":
                pl["LowLimitPrice"] = snap.getf(
                        Fields.MDEntryPx, i, Fields.MDEntryType)
                continue
            zm_et = CTSTICK_TO_ZMTICK[et]
            if zm_et not in sub["MDReqGrp"]:
                continue
            d = {}
            d["MDEntryType"] = zm_et
            d["MDEntryPx"] = msg.getf(
                    Fields.MDEntryPx, i, Fields.MDEntryType)
            d["MDEntrySize"] = msg.getf(
                    Fields.MDEntrySize, i, Fields.MDEntryType)
            agg = msg.gets(Fields.TickDirection, i, Fields.MDEntryType)
            d["AggressorSide"] = CTSAGG_TO_ZMAGG.get(agg)
            d["ZMTickerID"] = tid
            d = {k: v for k, v in d.items() if v is not None}
            # TradeTime is very inaccurate (can be in future) so it is skipped
            # TODO: emit extra optional fields that appear sometimes ...
            group.append(d)

        if fix.MDEntryType.TradeVolume in sub["MDReqGrp"]:
            d = {}
            d["MDEntryType"] = fix.MDEntryType.TradeVolume
            d["MDEntrySize"] = msg.getf(Fields.TotalVolumeTraded)
            d["ZMTickerID"] = tid
            group.append(d)

        data = " " + json.dumps(d)
        data = [b"X", data.encode()]
        await g.sock_pub.send_multipart(data)

        if pl:
            data = {"PriceLimits": pl}
            await self._emit_security_status(sec_id, sub, data)


    async def handle_md_inc_refresh(self, msg):

        sec_id = msg.gets(Fields.SecurityID)
        if sec_id not in g.cts_secid_to_insid:
            L.warning("SecurityID not subscribed: {}".format(sec_id))
            return

        if g.debug_mode:
            # should be only one instrument per X message in CTS FIX
            if msg.get_raw(Fields.SecurityID, 2):
                L.critical("two SecurityIDs found in X message:\n{}"
                           .format(msg))
                sys.exit(1)

        sub = g.subscriptions[sec_id]

        await self._flush_snap_buffer(sub)

        sec_id = msg.gets(Fields.SecurityID)
        sts = int(msg.get(Fields.SecurityTradingStatus))
        await self._emit_trading_session(sec_id, sub, sts)

        num_entries = msg.get(Fields.NoMDEntries)
        if not num_entries:
            return
        num_entries = int(num_entries)

        seq_no = g.seq_no
        g.seq_no += 1
        d = {}
        d["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = get_timestamp()
        d["Body"] = body = {}
        ttime = msg.gets(Fields.SendingTime)
        ttime = datetime.strptime(ttime, "%Y%m%d-%H:%M:%S.%f")
        ttime = int(ttime.timestamp() * 1e9)
        ins_id = g.cts_secid_to_insid[sec_id]
        tid = g.ctl.insid_to_tid[ins_id]
        body["SendingTime"] = ttime
        body["MDIncGrp"] = group = []

        try:
            for i in range(1, num_entries + 1):

                post_entries = []

                et = msg.gets(Fields.MDEntryType, i, Fields.MDUpdateAction)
                zm_et = CTSTICK_TO_ZMTICK.get(et)
                if not zm_et:
                    L.error("{}: unknown MDEntryType '{}'".format(sec_id, et))
                    continue

                if zm_et in "01EF":
                    ua = msg.gets(Fields.MDUpdateAction, i,
                                  Fields.MDUpdateAction)
                    pos = int(msg.get(Fields.MDEntryLevel, i,
                                      Fields.MDUpdateAction)) - 1
                    entry_px = msg.get(Fields.MDEntryPx, i,
                                       Fields.MDUpdateAction)
                    if entry_px:
                        entry_px = int(entry_px)
                    prices = sub["prices"][zm_et]
                    L.debug("et: {}, ua: {}, pos: {}, entry_px: {}, prices: {}"
                            .format(zm_et, ua, pos, entry_px, prices))
                    if ua == "0":
                        if entry_px is None:
                            L.error("{}: MDEntryPx not provided for insert"
                                    .format(sec_id))
                            continue
                        prices.insert(pos, entry_px)
                        for i2 in range(sub["max_levels"],
                                        len(sub["prices"][zm_et])):
                            entry2 = {}
                            entry2["MDUpdateAction"] = "2"
                            entry2["MDEntryType"] = et
                            entry2["MDPriceLevel"] = i2 + 1
                            entry2["MDEntryPx"] = sub["prices"][zm_et][i2]
                            entry2["ZMTickerID"] = tid
                            post_entries.append(entry2)
                        sub["prices"][zm_et] = \
                                sub["prices"][zm_et][:sub["max_levels"]]
                    elif ua == "1":
                        if entry_px is not None:
                            L.warning("{}: MDEntryPx provided for change"
                                      .format(sec_id))
                        try:
                            entry_px = prices[pos]
                        except IndexError:
                            L.error("{}: Tried to update non-existing pos"
                                    .format(sec_id))
                            continue
                    elif ua == "2":
                        if entry_px is not None:
                            L.warning("{}: MDEntryPx provided for remove"
                                      .format(sec_id))
                        try:
                            entry_px = prices[pos]
                            del prices[pos]
                        except IndexError:
                            L.error("{}: Tried to delete non-existing pos"
                                    .format(sec_id))
                            continue
                    else:
                        L.warning("{}: unknown MDUpdateAction '{}'"
                                  .format(sec_id, ua))
                        continue
                    L.debug("entry_px: {}".format(entry_px))

                entry = {}
                entry["MDUpdateAction"] = ua
                entry["MDEntryType"] = zm_et
                if entry_px is not None:
                    entry["MDEntryPx"] = float(entry_px)
                entry_size = msg.get(Fields.MDEntrySize, i,
                                     Fields.MDUpdateAction)
                if entry_size is not None:
                    entry["MDEntrySize"] = float(entry_size)
                if pos is not None:
                    entry["MDPriceLevel"] = pos
                entry["ZMTickerID"] = tid

                group.append(entry)

                if post_entries:
                    L.debug("{} post entries".format(len(post_entries)))
                    group += post_entries

        except:
            sys.exit(1)

        data = " " + json.dumps(d)
        data = [b"X", data.encode()]
        await g.sock_pub.send_multipart(data)


    async def listen_topics_until(self, topics, term_pred, timeout=-1):
        sock = g.ctx.socket(zmq.SUB)
        sock.connect(self.PUB_ADDR)
        for topic in topics:
            sock.subscribe(topic)
        poller = zmq.asyncio.Poller()
        poller.register(sock, zmq.POLLIN)
        parser = FixParser()
        tic = time()
        res = []
        while True:
            if timeout >= 0:
                remaining = timeout - ((time() - tic) * 1000)
                if remaining <= 0:
                    break
                evs = await poller.poll(remaining)
                if not evs:
                    break
            msg_parts = await sock.recv_multipart()
            msg_type = msg_parts[0]
            parser.append_buffer(msg_parts[1])
            msg = parser.get_message()
            res.append(msg)
            if term_pred(msg):
                break
        sock.close()
        return res


    HANDLERS = {
        fix.MsgType.TestRequest: handle_test_request,
        fix.MsgType.Logon: handle_logon,
        fix.MsgType.SecurityDefinition: handle_security_definition,
        fix.MsgType.MarketDataIncrementalRefresh: handle_md_inc_refresh,
        fix.MsgType.MarketDataSnapshotFullRefresh: handle_md_snap_full_refresh,
    }


###############################################################################


class FIXClient:


    def __init__(self, settings):
        self._settings = settings
        self._reader = None
        self._writer = None
        self._sec_req_id = 0
        self._test_req_id = 0
        self._md_req_id = 0


    def create_fix_msg(self, msg_type):
        msg = simplefix.FixMessage()
        msg.append_pair(Fields.BeginString, "FIX 4.2")
        msg.append_pair(Fields.MsgType, msg_type)
        msg.append_pair(Fields.SenderCompID, self._settings["SenderCompID"])
        msg.append_pair(Fields.TargetCompID, self._settings["TargetCompID"])
        secure_data = self._settings["SecureData"]
        msg.append_pair(Fields.SecureData, secure_data)
        msg.append_pair(Fields.SecureDataLen, len(secure_data))
        return msg


    def _send_fix_msg(self, msg):
        seq_num = g.fix_seq_num_tx
        g.fix_seq_num_tx += 1
        msg.append_pair(Fields.MsgSeqNum, seq_num)
        sending_time = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
        msg.append_pair(Fields.SendingTime, sending_time)
        L.debug("> {}".format(msg))
        self._writer.write(msg.encode())


    async def _connect(self):

        L.debug("FIXClient: connecting ...")

        sslctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        sslctx.verify_mode = ssl.CERT_REQUIRED
        sslctx.load_default_certs()
        reader, self._writer = await asyncio.open_connection(
                self._settings["IP"], self._settings["Port"], ssl=sslctx)
        self._reader = FIXReader(reader)
        create_task(self._reader.read_forever())

        L.debug("FIXClient: sending Logon ...")
        msg = self.create_fix_msg(fix.MsgType.Logon)
        msg.append_pair(Fields.HeartBtInt, 30)  # heartbeat interval in secs
        msg.append_pair(Fields.EncryptMethod, 0)  # no encryption
        msg.append_pair(Fields.UserName, self._settings["UserName"])
        msg.append_pair(Fields.Password, self._settings["Password"])
        # no disconnects without any special parameters??

        msg.append_pair(Fields.NoMsgTypes, 1)
        # # disable portfolio orders, positions and account details listing
        # msg.append_pair(Fields.RefMsgType, "d")
        # enable security definition requests
        msg.append_pair(Fields.RefMsgType, "c")
        # # disable automatic subscription to all accounts
        # msg.append_pair(Fields.RefMsgType, "BB")
        # # enable chart data requests
        # msg.append_pair(Fields.RefMsgType, "V")
        # enable decimal pricing format (orders only?)
        # msg.append_pair(Fields.RefMsgType, "D")
        self._send_fix_msg(msg)


    async def run(self):
        await self._connect()


    def send_heartbeat(self, req_id=None):
        msg = self.create_fix_msg(fix.MsgType.Heartbeat)
        if req_id:
            msg.append_pair(Fields.TestReqID, req_id)
        self._send_fix_msg(msg)


    def send_test_request(self, req_id=None):
        if not req_id:
            req_id = self._test_req_id
            self._test_req_id += 1
        msg = self.create_fix_msg(fix.MsgType.TestRequest)
        msg.append_pair(Fields.TestReqID, req_id)
        self._send_fix_msg(msg)


    async def pinger(self):
        L.debug("FIXClient: test_request_sender started")
        while self._reader.connected:
            self.send_heartbeat()
            await asyncio.sleep(20)


    async def security_definition_request(self, data):

        with tcache.open(g.secdefs_cache, "r", timedelta(days=1)) as c:
            key = dict_to_key(data)
            res = c.get(key)
            if res:
                L.debug("cache hit: {} results".format(len(res)))
                return res

        msg = self.create_fix_msg(fix.MsgType.SecurityDefinitionRequest)
        msg.append_pair(Fields.SecurityType, data["SecurityType"])
        if "SecurityExchange" in data:
            msg.append_pair(Fields.SecurityExchange, data["SecurityExchange"])
        if "PutOrCall" in data:
            msg.append_pair(Fields.PutOrCall, data["PutOrCall"])
        if "Symbol" in data:
            msg.append_pair(Fields.Symbol, data["Symbol"])
        if "SecuritySubType" in data:
            msg.append_pair(Fields.SecuritySubType, data["SecuritySubType"])
        if "SecurityID" in data:
            msg.append_pair(Fields.SecurityID, data["SecurityID"])
        if "SecurityRequestType" in data:
            msg.append_pair(Fields.SecurityRequestType,
                            data["SecurityRequestType"])
        else:
            # REQUEST_LIST_SECURITIES
            msg.append_pair(Fields.SecurityRequestType, 3)
        if "MaturityMonthYear" in data:
            msg.append_pair(Fields.MaturityMonthYear,
                            data["MaturityMonthYear"])

        sec_req_id = self._sec_req_id
        self._sec_req_id += 1
        msg.append_pair(Fields.SecurityReqID, sec_req_id)
        sec_req_id_bytes = msg.get(Fields.SecurityReqID)

        # Multiple simulateous ongoing SecurityDefinitionRequests may result
        # in forced disconnection ...
        async with g.sec_def_lock:

            self._send_fix_msg(msg)

            topics = [fix.MsgType.SecurityDefinition]
            term_state = {
                "count": 0
            }
            def term_pred(msg):
                max_count = int(msg.get(Fields.TotNumReports))
                if msg.get(Fields.SecurityReqID) == sec_req_id_bytes:
                    term_state["count"] += 1
                    # L.debug("count: {}".format(term_state["count"]))
                if term_state["count"] >= max_count:
                    L.debug("{} SecurityDefinition messages received"
                            .format(term_state["count"]))
                    return True
                return False
            res = await self._reader.listen_topics_until(topics, term_pred)
            res = [x for x in res
                   if x.get(Fields.SecurityReqID) == sec_req_id_bytes]

            if len(res) == 1 \
                    and res[0].gets(Fields.SecurityResponseType) == "5":
                # Reject Security Proposal
                err_msg = res[0].gets(Fields.SecurityDesc)
                raise BusinessMessageRejectException("Rejected: " + err_msg)

            with tcache.open(g.secdefs_cache, "w", timedelta(days=1)) as c:
                key = dict_to_key(data)
                c[key] = res

            return res

    
    async def send_market_data_request(self, data):

        msg = self.create_fix_msg(fix.MsgType.MarketDataRequest)
        msg.append_pair(Fields.SecurityType, data["SecurityType"])
        msg.append_pair(Fields.SecurityExchange, data["SecurityExchange"])
        msg.append_pair(Fields.Symbol, data["Symbol"])
        msg.append_pair(Fields.SecurityID, data["SecurityID"])
        if "MDUpdateType" in data:
            msg.append_pair(Fields.MDUpdateType, data["MDUpdateType"])
        msg.append_pair(Fields.MarketDepth, data["MarketDepth"])
        msg.append_pair(Fields.NoMDEntryTypes, len(data["MDReqGrp"]))
        for c in data["MDReqGrp"]:
            msg.append_pair(Fields.MDEntryType, c)

        sub = g.subscriptions[data["SecurityID"]]

        if "md_req_id" in sub:
            md_req_id = sub["md_req_id"]
        else:
            md_req_id = self._md_req_id
            self._md_req_id += 1
            sub["md_req_id"] = md_req_id
        msg.append_pair(Fields.MDReqID, md_req_id)

        if data["SubscriptionRequestType"] == "7":  # snapshot and updates

            if sub["snapshot_buffer"]:
                return "subscription pending"

            if sub["initial_snapshot_completed"]:
                # Unsubscribe first to prevent X update from coming
                # before W series is finished. Is this the best solution?
                msg_unsub = deepcopy(msg)
                msg_unsub.append_pair(Fields.SubscriptionRequestType, "2")
                self._send_fix_msg(msg_unsub)
                L.debug("waiting for a bit ...")
                await asyncio.sleep(5)

            msg.append_pair(Fields.SubscriptionRequestType, "7")
            self._send_fix_msg(msg)

            # If instrument is resubscribed, permit sending new snapshot as
            # well.
            sub["snapshot_buffer"].clear()
            sub["initial_snapshot_completed"] = False
            sub["last_snap_received"] = 0
            sub["max_levels"] = data["MarketDepth"]

            topics = [
                fix.MsgType.MarketDataRequestReject,
                fix.MsgType.MarketDataSnapshotFullRefresh,
            ]
            def term_pred(msg):
                if md_req_id == msg.gets(Fields.MDReqID) \
                        or data["SecurityID"] == msg.gets(Fields.SecurityID):
                    return True
                return False
            res = await self._reader.listen_topics_until(
                    topics, term_pred, timeout=10*1000)
            if not res:
                return "subscribed, no confirmation"
            res = res[0]
            if res.gets(Fields.MsgType) == fix.MsgType.MarketDataRequestReject:
                # TODO: specify reason
                raise MarketDataRequestRejectException(
                        "{} ({})".format(res.gets(Fields.Text),
                                         res.gets(Fields.MDReqRejReason)))
            return "success"

        if data["SubscriptionRequestType"] == "2":
            msg.append_pair(Fields.SubscriptionRequestType, "2")
            self._send_fix_msg(msg)
            # There is no way to confirm; no errors or success 
            # messages are emitted.
            # g.sub_locks[g.cts_secid_to_insid[data["SecurityID"]]].release()
            return "success"



###############################################################################


def parse_args():
    parser = argparse.ArgumentParser(description="CTS FIX AC/MD connector")
    parser.add_argument("profile",
                        help="profile to use")
    parser.add_argument("md_ctl_addr",
                        help="address to bind to for MD ctl socket")
    parser.add_argument("md_pub_addr",
                        help="address to bind to for MD pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    setup_root_logger(args.log_level)


def get_working_dirs(args):
    g.zmapi_dir = get_zmapi_dir()
    g.app_dir = os.path.join(g.zmapi_dir, "ctsfix")
    g.profile_dir = os.path.join(g.app_dir, "profile_" + args.profile)
    g.cache_dir = os.path.join(g.profile_dir, "cache")
    makedirs(g.cache_dir)
    g.secdefs_cache = os.path.join(g.cache_dir, "secdefs.cache")
    tcache.ensure_exists(g.secdefs_cache)


def load_settings():
    fn = os.path.join(g.profile_dir, "settings.json")
    with open(fn) as f:
        return json.load(f)


def init_zmq_sockets(args):
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.md_ctl_addr)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.md_pub_addr)


def patch_simplefix():
    def get(self, tag, nth=1, sep=None, default=None):
        tag = simplefix.message.fix_tag(tag)
        if sep:
            sep = simplefix.message.fix_tag(sep)
        if nth > 1 and sep is None:
            raise ValueError("sep must be provided when nth > 1")
        for t, v in self.pairs:
            if t == sep:
                nth -= 1
            if t == tag:
                if sep is None or nth == 0:
                    return v
        return default
    def gets(self, tag, nth=1, sep=None, default=None):
        res = self.get(tag, nth=nth, sep=sep, default=default)
        if res:
            return res.decode()
        return res
    def geti(self, tag, nth=1, sep=None, default=None):
        res = self.get(tag, nth=nth, sep=sep, default=default)
        if res:
            return int(res)
        return res
    def getf(self, tag, nth=1, sep=None, default=None):
        res = self.get(tag, nth=nth, sep=sep, default=default)
        if res:
            return float(res)
        return res
    def fixmessage_str(self):
        s = "FixMessage ({})\n".format(self.gets(35))
        for tag, value in self.pairs:
            if tag == b"35":
                continue
            s += "  " + tag.decode() + "=" + value.decode() + "\n"
        s = s[:-1]
        return s
    FixMessage.get_raw = FixMessage.get
    FixMessage.get = get
    FixMessage.gets = gets
    FixMessage.geti = geti
    FixMessage.getf = getf
    FixMessage.__str__ = fixmessage_str


def main():
    args = parse_args()
    setup_logging(args)
    patch_simplefix()
    get_working_dirs(args)
    settings = load_settings()
    init_zmq_sockets(args)
    g.ctl = MDController(g.sock_ctl)
    g.fix_client = FIXClient(settings)
    L.debug("starting event loop ...")
    tasks = [
        delayed(g.ctl.run, g.startup_event),
        g.fix_client.run(),
    ]
    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass
    L.debug("destroying zmq context ...")
    g.ctx.destroy()


if __name__ == "__main__":
    main()

