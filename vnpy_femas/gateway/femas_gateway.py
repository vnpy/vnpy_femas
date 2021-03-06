""""""

from typing import Callable, Dict, List
import pytz
from datetime import datetime
from time import sleep

from ..api import (
    MdApi,
    TdApi,
    USTP_FTDC_AF_Delete,
    USTP_FTDC_CAS_Accepted,
    USTP_FTDC_CAS_Rejected,
    USTP_FTDC_CAS_Submitted,
    USTP_FTDC_CHF_Speculation,
    USTP_FTDC_D_Buy,
    USTP_FTDC_D_Sell,
    USTP_FTDC_FCR_NotForceClose,
    USTP_FTDC_OF_Close,
    USTP_FTDC_OF_CloseToday,
    USTP_FTDC_OF_CloseYesterday,
    USTP_FTDC_OF_Open,
    USTP_FTDC_OPT_AnyPrice,
    USTP_FTDC_OPT_LimitPrice,
    USTP_FTDC_OS_AllTraded,
    USTP_FTDC_OS_Canceled,
    USTP_FTDC_OS_NoTradeQueueing,
    USTP_FTDC_OS_PartTradedQueueing,
    USTP_FTDC_OT_CallOptions,
    USTP_FTDC_OT_PutOptions,
    USTP_FTDC_TC_GFD,
    USTP_FTDC_TC_IOC,
    USTP_FTDC_VC_AV,
    USTP_FTDC_VC_CV
)
from vnpy.event.engine import EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Offset,
    OptionType,
    OrderType,
    Status,
    Product
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    CancelRequest,
    ContractData,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.utility import get_folder_path


STATUS_FEMAS2VT = {
    USTP_FTDC_CAS_Submitted: Status.SUBMITTING,
    USTP_FTDC_CAS_Accepted: Status.SUBMITTING,
    USTP_FTDC_CAS_Rejected: Status.REJECTED,
    USTP_FTDC_OS_NoTradeQueueing: Status.NOTTRADED,
    USTP_FTDC_OS_PartTradedQueueing: Status.PARTTRADED,
    USTP_FTDC_OS_AllTraded: Status.ALLTRADED,
    USTP_FTDC_OS_Canceled: Status.CANCELLED,
}

DIRECTION_VT2FEMAS = {
    Direction.LONG: USTP_FTDC_D_Buy,
    Direction.SHORT: USTP_FTDC_D_Sell,
}
DIRECTION_FEMAS2VT = {v: k for k, v in DIRECTION_VT2FEMAS.items()}

ORDERTYPE_VT2FEMAS = {
    OrderType.LIMIT: USTP_FTDC_OPT_LimitPrice,
    OrderType.MARKET: USTP_FTDC_OPT_AnyPrice,
}

OFFSET_VT2FEMAS = {
    Offset.OPEN: USTP_FTDC_OF_Open,
    Offset.CLOSE: USTP_FTDC_OF_Close,
    Offset.CLOSETODAY: USTP_FTDC_OF_CloseYesterday,
    Offset.CLOSEYESTERDAY: USTP_FTDC_OF_CloseToday,
}
OFFSET_FEMAS2VT = {v: k for k, v in OFFSET_VT2FEMAS.items()}

EXCHANGE_FEMAS2VT = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
}

OPTIONTYPE_FEMAS2VT = {
    USTP_FTDC_OT_CallOptions: OptionType.CALL,
    USTP_FTDC_OT_PutOptions: OptionType.PUT,
}

CHINA_TZ = pytz.timezone("Asia/Shanghai")


symbol_contract_map: Dict[str, ContractData] = {}


class FemasGateway(BaseGateway):
    """
    VeighNa?????????????????????????????????
    """

    default_name: str = "FEMAS"

    default_setting: dict = {
        "?????????": "",
        "??????": "",
        "???????????????": "",
        "???????????????": "",
        "???????????????": "",
        "????????????": "",
        "????????????": "",
    }

    exchanges: List[str] = list(EXCHANGE_FEMAS2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """????????????"""
        super().__init__(event_engine, gateway_name)

        self.td_api: FemasTdApi = FemasTdApi(self)
        self.md_api: FemasTdApi = FemasMdApi(self)

    def connect(self, setting: dict) -> None:
        """??????????????????"""
        userid: str = setting["?????????"]
        password: str = setting["??????"]
        brokerid: str = setting["???????????????"]
        td_address: str = setting["???????????????"]
        md_address: str = setting["???????????????"]

        if not td_address.startswith("tcp://"):
            td_address = "tcp://" + td_address
        if not md_address.startswith("tcp://"):
            md_address = "tcp://" + md_address

        appid: str = setting["????????????"]
        auth_code: str = setting["????????????"]
        

        self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid)
        self.md_api.connect(md_address, userid, password, brokerid)

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """????????????"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> None:
        """????????????"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """????????????"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """????????????"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """????????????"""
        self.td_api.query_position()

    def close(self) -> None:
        """????????????"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, error: dict) -> None:
        """????????????????????????"""
        error_id: str = error["ErrorID"]
        error_msg: str = error["ErrorMsg"]
        msg: str = f"{msg}????????????{error_id}????????????{error_msg}"
        self.write_log(msg)

    def process_timer_event(self, event) -> None:
        """??????????????????"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func: Callable = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """?????????????????????"""
        self.count: int = 0
        self.query_functions: List[Callable] = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class FemasMdApi(MdApi):
    """"""

    def __init__(self, gateway: FemasGateway) -> None:
        """????????????"""
        super(FemasMdApi, self).__init__()

        self.gateway: FemasGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_staus: bool = False
        self.login_failed: bool = False

        self.subscribed: List[str] = set()

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: int = 0

    def onFrontConnected(self) -> None:
        """???????????????????????????"""
        self.gateway.write_log("???????????????????????????")
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """???????????????????????????"""
        self.login_status = False
        self.gateway.write_log(f"????????????????????????????????????{reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """????????????????????????"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("???????????????????????????")

            for symbol in self.subscribed:
                self.subMarketData(symbol)
        else:
            self.gateway.write_error("???????????????????????????", error)

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        """??????????????????"""
        self.gateway.write_error("??????????????????", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """??????????????????"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error("??????????????????", error)

    def onRtnDepthMarketData(self, data: dict) -> None:
        """??????????????????"""
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        timestamp: str = f"{data['TradingDay']} {data['UpdateTime']}.{int(data['UpdateMillisec'] / 100)}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt = CHINA_TZ.localize(dt)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=contract.exchange,
            datetime=dt,
            name=contract.name,
            volume=data["Volume"],
            last_price=data["LastPrice"],
            limit_up=data["UpperLimitPrice"],
            limit_down=data["LowerLimitPrice"],
            open_price=data["OpenPrice"],
            high_price=data["HighestPrice"],
            low_price=data["LowestPrice"],
            pre_close=data["PreClosePrice"],
            bid_price_1=data["BidPrice1"],
            ask_price_1=data["AskPrice1"],
            bid_volume_1=data["BidVolume1"],
            ask_volume_1=data["AskVolume1"],
            gateway_name=self.gateway_name,
        )
        self.gateway.on_tick(tick)

    def connect(self, address: str, userid: str, password: str, brokerid: int) -> None:
        """???????????????"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # ????????????????????????????????????????????????
        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi((str(path) + "\\Md").encode("GBK"))

            self.subscribeMarketDataTopic(100, 2)
            self.registerFront(address)
            self.init()

            self.connect_status = True
        # ???????????????????????????????????????
        elif not self.login_status:
            self.login()

    def login(self) -> None:
        """????????????"""
        req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
        }

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def subscribe(self, req: SubscribeRequest) -> None:
        """????????????"""
        if self.login_status:
            self.subMarketData(req.symbol)
        self.subscribed.add(req.symbol)

    def close(self) -> None:
        """????????????"""
        if self.connect_status:
            self.exit()


class FemasTdApi(TdApi):
    """"""

    def __init__(self, gateway: FemasGateway):
        """????????????"""
        super(FemasTdApi, self).__init__()

        self.gateway: FemasGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0
        self.localid: int = int(10e5 + 8888)

        self.connect_status: bool = False
        self.login_status: bool = False
        self.login_failed: bool = False
        self.login_status: bool = False

        self.userid: str = ""
        self.investorid: str = ""
        self.password: str = ""
        self.brokerid: int = 0
        self.auth_code: str = ""
        self.appid: str = ""
        

        self.positions: dict = {}
        self.tradeids: List[str] = set()

    def onFrontConnected(self) -> None:
        """???????????????????????????"""
        self.gateway.write_log("???????????????????????????")

        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """???????????????????????????"""
        self.login_status = False
        self.gateway.write_log(f"????????????????????????????????????{reason}")

    def onRspDSUserCertification(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """????????????????????????"""
        if not error["ErrorID"]:
            self.auth_staus = True
            self.gateway.write_log("?????????????????????????????????")
            self.login()
        else:
            self.gateway.write_error("?????????????????????????????????", error)

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """????????????????????????"""
        if not error["ErrorID"]:
            if data["MaxOrderLocalID"]:
                self.localid = int(data["MaxOrderLocalID"])

            self.login_status = True
            self.gateway.write_log("???????????????????????????")

            self.query_investor()
        else:
            self.login_failed = True

            self.gateway.write_error("???????????????????????????", error)

    def onRspQryUserInvestor(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """?????????????????????????????????"""
        self.investorid = data['InvestorID']
        self.gateway.write_log("???????????????????????????")

        sleep(1)    # ?????????????????????????????????1??????
        self.reqid += 1
        self.reqQryInstrument({}, self.reqid)

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """????????????????????????"""
        if not error["ErrorID"]:
            return

        orderid:str = data["UserOrderLocalID"]
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            direction=DIRECTION_FEMAS2VT[data["Direction"]],
            offset=OFFSET_FEMAS2VT[data["OffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["Volume"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name,
        )
        self.gateway.on_order(order)

        self.gateway.write_error("??????????????????", error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """????????????????????????"""
        if not error["ErrorID"]:
            return

        self.gateway.write_error("??????????????????", error)

    def onRspQueryMaxOrderVolume(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """"""
        pass

    def onRspSettlementInfoConfirm(
        self, data: dict, error: dict, reqid: int, last: bool
    ) -> None:
        """?????????????????????"""
        self.gateway.write_log("????????????????????????")

        self.reqid += 1
        self.reqQryInstrument({}, self.reqid)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """??????????????????"""
        if not data:
            return

        # ??????????????????????????????????????????
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)

        if contract:
            # ???????????????????????????????????????
            key: str = f"{data['InstrumentID'], data['Direction']}"
            position: PositionData = self.positions.get(key, None)
            if not position:
                position = PositionData(
                    symbol=data["InstrumentID"],
                    exchange=contract.exchange,
                    direction=DIRECTION_FEMAS2VT[data["Direction"]],
                    gateway_name=self.gateway_name,
                )
                self.positions[key] = position

            position.yd_volume = data["YdPosition"]
            # ??????????????????????????????????????????
            cost: float = position.price * position.volume

            # ????????????????????????
            position.volume += data["Position"]

            # ??????????????????????????????????????????
            if position.volume:
                cost += data["PositionCost"]
                position.price = cost / position.volume

            # ????????????????????????
            position.frozen += data["FrozenPosition"]

        if last:
            for position in self.positions.values():
                self.gateway.on_position(position)

            self.positions.clear()

    def onRspQryInvestorAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """??????????????????"""
        account: AccountData = AccountData(
            accountid=data["AccountID"],
            frozen=data["LongMargin"] + data["ShortMargin"],
            balance=data["PreBalance"],
            gateway_name=self.gateway_name,
        )

        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """??????????????????"""
        # ????????????????????????ProductClass????????????????????????????????????????????????????????????  
        option_type: OptionType = OPTIONTYPE_FEMAS2VT.get(data["OptionsType"], None)
        if option_type:
            product = Product.OPTION
        elif data["InstrumentID_2"]:
            product = Product.SPREAD
        else:
            product = Product.FUTURES

        contract: ContractData = ContractData(
            symbol=data["InstrumentID"],
            exchange=EXCHANGE_FEMAS2VT[data["ExchangeID"]],
            name=data["InstrumentName"],
            size=data["VolumeMultiple"],
            pricetick=data["PriceTick"],
            product=product,
            gateway_name=self.gateway_name
        )

        if product == Product.OPTION:
            # ??????????????????????????????????????????C/P??????
            if contract.exchange == Exchange.CZCE:
                contract.option_portfolio = data["ProductID"][:-1]
            else:
                contract.option_portfolio = data["ProductID"]

            contract.option_underlying = data["UnderlyingInstrID"]
            contract.option_type = OPTIONTYPE_FEMAS2VT.get(data["OptionsType"], None)
            contract.option_strike = data["StrikePrice"]
            contract.option_index = str(data["StrikePrice"])
            contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d")

        self.gateway.on_contract(contract)

        symbol_contract_map[contract.symbol] = contract

        if last:
            self.gateway.write_log("????????????????????????")

    def onRtnOrder(self, data: dict) -> None:
        """??????????????????"""
        timestamp: str = f"{data['InsertDate']} {data['InsertTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt = CHINA_TZ.localize(dt)

        order: OrderData = OrderData(
            symbol=data["InstrumentID"],
            exchange=EXCHANGE_FEMAS2VT[data["ExchangeID"]],
            orderid=data["UserOrderLocalID"],
            direction=DIRECTION_FEMAS2VT[data["Direction"]],
            offset=OFFSET_FEMAS2VT[data["OffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["Volume"],
            traded=data["VolumeTraded"],
            status=STATUS_FEMAS2VT[data["OrderStatus"]],
            datettime=dt,
            gateway_name=self.gateway_name,
        )

        self.localid = max(self.localid, int(order.orderid))
        self.gateway.on_order(order)

    def onRtnTrade(self, data: dict) -> None:
        """??????????????????"""
        # ??????????????????????????????
        tradeid: str = data["TradeID"]
        if tradeid in self.tradeids:
            return
        self.tradeids.add(tradeid)

        timestamp: str = f"{data['TradeDate']} {data['TradeTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt = CHINA_TZ.localize(dt)

        trade: OrderData = TradeData(
            symbol=data["InstrumentID"],
            exchange=EXCHANGE_FEMAS2VT[data["ExchangeID"]],
            orderid=data["UserOrderLocalID"],
            tradeid=tradeid,
            direction=DIRECTION_FEMAS2VT[data["Direction"]],
            offset=OFFSET_FEMAS2VT[data["OffsetFlag"]],
            price=data["TradePrice"],
            volume=data["TradeVolume"],
            datetime=dt,
            gateway_name=self.gateway_name,
        )

        self.gateway.on_trade(trade)

    def connect(
        self,
        address: str,
        userid: str,
        password: str,
        brokerid: int,
        auth_code: str,
        appid: str,
  
    ) -> None:
        """???????????????"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.address = address
        self.auth_code = auth_code
        self.appid = appid
        

        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcTraderApi(str(path) + "\\Td")

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)
            self.subscribeUserTopic(0)

            self.registerFront(address)
            self.init()

            self.connect_status = True
        else:
            self.authenticate()

    def authenticate(self) -> None:
        """??????????????????"""
        req: dict = {
            "AppID": self.appid,
            "AuthCode": self.auth_code,
            "EncryptType": "1",
        }

        

        self.reqid += 1
        self.reqDSUserCertification(req, self.reqid)

    def login(self) -> None:
        """????????????"""
        if self.login_failed:
            return

        req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
            "AppID": self.appid
        }

        

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def query_investor(self) -> None:
        """???????????????????????????"""
        self.reqid += 1

        req = {
            "BrokerID": self.brokerid,
            "UserID": self.userid,
        }

        self.reqQryUserInvestor(req, self.reqid)

    def send_order(self, req: OrderRequest) -> str:
        """????????????"""
        if req.offset not in OFFSET_VT2FEMAS:
            self.gateway.write_log("?????????????????????")
            return ""

        self.localid += 1
        orderid: str = str(self.localid).rjust(12, "0")

        femas_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": str(req.exchange).split(".")[1],
            "BrokerID": self.brokerid,
            "InvestorID": self.investorid,
            "UserID": self.userid,
            "LimitPrice": req.price,
            "Volume": int(req.volume),
            "OrderPriceType": ORDERTYPE_VT2FEMAS.get(req.type, ""),
            "Direction": DIRECTION_VT2FEMAS.get(req.direction, ""),
            "OffsetFlag": OFFSET_VT2FEMAS.get(req.offset, ""),
            "UserOrderLocalID": orderid,
            "HedgeFlag": USTP_FTDC_CHF_Speculation,
            "ForceCloseReason": USTP_FTDC_FCR_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": USTP_FTDC_TC_GFD,
            "VolumeCondition": USTP_FTDC_VC_AV,
            "MinVolume": 1,
        }

        if req.type == OrderType.FAK:
            femas_req["OrderPriceType"] = USTP_FTDC_OPT_LimitPrice
            femas_req["TimeCondition"] = USTP_FTDC_TC_IOC
            femas_req["VolumeCondition"] = USTP_FTDC_VC_AV
        elif req.type == OrderType.FOK:
            femas_req["OrderPriceType"] = USTP_FTDC_OPT_LimitPrice
            femas_req["TimeCondition"] = USTP_FTDC_TC_IOC
            femas_req["VolumeCondition"] = USTP_FTDC_VC_CV

        self.reqid += 1
        self.reqOrderInsert(femas_req, self.reqid)

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """????????????"""
        self.localid += 1
        orderid: str = str(self.localid).rjust(12, "0")

        femas_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": str(req.exchange).split(".")[1],
            "UserOrderLocalID": req.orderid,
            "UserOrderActionLocalID": orderid,
            "ActionFlag": USTP_FTDC_AF_Delete,
            "BrokerID": self.brokerid,
            "InvestorID": self.investorid,
            "UserID": self.userid,
        }

        self.reqid += 1
        self.reqOrderAction(femas_req, self.reqid)

    def query_account(self) -> None:
        """????????????"""
        if not self.investorid:
            return

        req: dict = {
            "BrokerID": self.brokerid,
            "InvestorID": self.investorid,
            "UserID": self.userid,
        }
        self.reqid += 1

        self.reqQryInvestorAccount(req, self.reqid)

    def query_position(self) -> None:
        """????????????"""
        if not symbol_contract_map:
            return

        req: dict = {
            "BrokerID": self.brokerid,
            "InvestorID": self.investorid,
            "UserID": self.userid,
        }

        self.reqid += 1
        self.reqQryInvestorPosition(req, self.reqid)

    def close(self) -> None:
        """????????????"""
        if self.connect_status:
            self.exit()
