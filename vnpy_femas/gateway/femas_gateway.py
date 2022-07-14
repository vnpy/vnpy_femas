from typing import Dict, List
from datetime import datetime
from time import sleep
from pathlib import Path

from vnpy.event import EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Offset,
    OptionType,
    OrderType,
    Status,
    Product
)
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
from vnpy.trader.utility import get_folder_path, ZoneInfo
from vnpy.trader.event import EVENT_TIMER

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


# 委托状态映射
STATUS_FEMAS2VT: Dict[str, Status] = {
    USTP_FTDC_CAS_Submitted: Status.SUBMITTING,
    USTP_FTDC_CAS_Accepted: Status.SUBMITTING,
    USTP_FTDC_CAS_Rejected: Status.REJECTED,
    USTP_FTDC_OS_NoTradeQueueing: Status.NOTTRADED,
    USTP_FTDC_OS_PartTradedQueueing: Status.PARTTRADED,
    USTP_FTDC_OS_AllTraded: Status.ALLTRADED,
    USTP_FTDC_OS_Canceled: Status.CANCELLED,
}

# 多空方向映射
DIRECTION_VT2FEMAS: Dict[Direction, str] = {
    Direction.LONG: USTP_FTDC_D_Buy,
    Direction.SHORT: USTP_FTDC_D_Sell,
}
DIRECTION_FEMAS2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2FEMAS.items()}

# 委托类型映射
ORDERTYPE_VT2FEMAS: Dict[OrderType, str] = {
    OrderType.LIMIT: USTP_FTDC_OPT_LimitPrice,
    OrderType.MARKET: USTP_FTDC_OPT_AnyPrice,
}

# 开平方向映射
OFFSET_VT2FEMAS: Dict[Offset, str] = {
    Offset.OPEN: USTP_FTDC_OF_Open,
    Offset.CLOSE: USTP_FTDC_OF_Close,
    Offset.CLOSETODAY: USTP_FTDC_OF_CloseYesterday,
    Offset.CLOSEYESTERDAY: USTP_FTDC_OF_CloseToday,
}
OFFSET_FEMAS2VT: Dict[str, Offset] = {v: k for k, v in OFFSET_VT2FEMAS.items()}

# 交易所映射
EXCHANGE_FEMAS2VT: Dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
}

# 期权类型映射
OPTIONTYPE_FEMAS2VT: Dict[str, OptionType] = {
    USTP_FTDC_OT_CallOptions: OptionType.CALL,
    USTP_FTDC_OT_PutOptions: OptionType.PUT,
}

# 其他常量
CHINA_TZ = ZoneInfo("Asia/Shanghai")       # 中国时区

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class FemasGateway(BaseGateway):
    """
    VeighNa用于连接飞马柜台的接口。
    """

    default_name: str = "FEMAS"

    default_setting: dict = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": "",
    }

    exchanges: List[str] = list(EXCHANGE_FEMAS2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.td_api: "FemasTdApi" = FemasTdApi(self)
        self.md_api: "FemasTdApi" = FemasMdApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        userid: str = setting["用户名"]
        password: str = setting["密码"]
        brokerid: str = setting["经纪商代码"]
        td_address: str = setting["交易服务器"]
        md_address: str = setting["行情服务器"]

        if not td_address.startswith("tcp://"):
            td_address = "tcp://" + td_address
        if not md_address.startswith("tcp://"):
            md_address = "tcp://" + md_address

        appid: str = setting["产品名称"]
        auth_code: str = setting["授权编码"]

        self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid)
        self.md_api.connect(md_address, userid, password, brokerid)

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> None:
        """委托下单"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def close(self) -> None:
        """关闭接口"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, error: dict) -> None:
        """输出错误信息日志"""
        error_id: str = error["ErrorID"]
        error_msg: str = error["ErrorMsg"]
        msg: str = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg)

    def process_timer_event(self, event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """初始化查询任务"""
        self.count: int = 0
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class FemasMdApi(MdApi):
    """"""

    def __init__(self, gateway: FemasGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: FemasGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_staus: bool = False
        self.login_failed: bool = False

        self.subscribed: set = set()

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: int = 0

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            for symbol in self.subscribed:
                self.subMarketData(symbol)
        else:
            self.gateway.write_error("行情服务器登录失败", error)

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        """请求报错回报"""
        self.gateway.write_error("行情接口报错", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """订阅行情回报"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error("行情订阅失败", error)

    def onRtnDepthMarketData(self, data: dict) -> None:
        """行情数据推送"""
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        timestamp: str = f"{data['TradingDay']} {data['UpdateTime']}.{int(data['UpdateMillisec'] / 100)}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt: datetime = dt.replace(tzinfo=CHINA_TZ)

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
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # 禁止重复发起连接，会导致异常崩溃
        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi((str(path) + "\\Md").encode("GBK"))

            self.subscribeMarketDataTopic(100, 2)
            self.registerFront(address)
            self.init()

            self.connect_status = True
        # 如果已经连接过了，直接登录
        elif not self.login_status:
            self.login()

    def login(self) -> None:
        """用户登录"""
        req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
        }

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if self.login_status:
            self.subMarketData(req.symbol)
        self.subscribed.add(req.symbol)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()


class FemasTdApi(TdApi):
    """"""

    def __init__(self, gateway: FemasGateway):
        """构造函数"""
        super().__init__()

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

        self.positions: Dict[str, PositionData] = {}
        self.tradeids: set = set()

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("交易服务器连接成功")

        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reason}")

    def onRspDSUserCertification(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户授权验证回报"""
        if not error["ErrorID"]:
            self.auth_staus = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.login()
        else:
            self.gateway.write_error("交易服务器授权验证失败", error)

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            if data["MaxOrderLocalID"]:
                self.localid = int(data["MaxOrderLocalID"])

            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")

            self.query_investor()
        else:
            self.login_failed = True

            self.gateway.write_error("交易服务器登录失败", error)

    def onRspQryUserInvestor(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托查询投资者代码回报"""
        self.investorid = data['InvestorID']
        self.gateway.write_log("投资者代码查询成功")

        sleep(1)    # 由于流量控制，需要等待1秒钟
        self.reqid += 1
        self.reqQryInstrument({}, self.reqid)

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托下单失败回报"""
        if not error["ErrorID"]:
            return

        orderid: str = data["UserOrderLocalID"]
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

        self.gateway.write_error("交易委托失败", error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托撤单失败回报"""
        if not error["ErrorID"]:
            return

        self.gateway.write_error("交易撤单失败", error)

    def onRspSettlementInfoConfirm(
        self, data: dict, error: dict, reqid: int, last: bool
    ) -> None:
        """确认结算单回报"""
        self.gateway.write_log("结算信息确认成功")

        self.reqid += 1
        self.reqQryInstrument({}, self.reqid)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """持仓查询回报"""
        if not data:
            return

        # 必须收到了合约信息后才能处理
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)

        if contract:
            # 获取之前缓存的持仓数据缓存
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
            # 计算之前已有仓位的持仓总成本
            cost: float = position.price * position.volume

            # 累加更新持仓数量
            position.volume += data["Position"]

            # 计算更新后的持仓总成本和均价
            if position.volume:
                cost += data["PositionCost"]
                position.price = cost / position.volume

            # 更新仓位冻结数量
            position.frozen += data["FrozenPosition"]

        if last:
            for position in self.positions.values():
                self.gateway.on_position(position)

            self.positions.clear()

    def onRspQryInvestorAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """资金查询回报"""
        account: AccountData = AccountData(
            accountid=data["AccountID"],
            frozen=data["LongMargin"] + data["ShortMargin"],
            balance=data["PreBalance"],
            gateway_name=self.gateway_name,
        )

        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """合约查询回报"""
        # 飞马柜台没有提供ProductClass数据，因此需要使用以下逻辑确定产品类型。
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
            # 移除郑商所期权产品名称带有的C/P后缀
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
            self.gateway.write_log("合约信息查询成功")

    def onRtnOrder(self, data: dict) -> None:
        """委托更新推送"""
        timestamp: str = f"{data['InsertDate']} {data['InsertTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = dt.replace(tzinfo=CHINA_TZ)

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
        """成交数据推送"""
        # 过滤重复交易数据推送
        tradeid: str = data["TradeID"]
        if tradeid in self.tradeids:
            return
        self.tradeids.add(tradeid)

        timestamp: str = f"{data['TradeDate']} {data['TradeTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = dt.replace(tzinfo=CHINA_TZ)

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
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.address = address
        self.auth_code = auth_code
        self.appid = appid

        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
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
        """发起授权验证"""
        req: dict = {
            "AppID": self.appid,
            "AuthCode": self.auth_code,
            "EncryptType": "1",
        }

        self.reqid += 1
        self.reqDSUserCertification(req, self.reqid)

    def login(self) -> None:
        """用户登录"""
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
        """委托查询可用投资者"""
        self.reqid += 1

        req: dict = {
            "BrokerID": self.brokerid,
            "UserID": self.userid,
        }

        self.reqQryUserInvestor(req, self.reqid)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in OFFSET_VT2FEMAS:
            self.gateway.write_log("请选择开平方向")
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
        """委托撤单"""
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
        """查询资金"""
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
        """查询持仓"""
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
        """关闭连接"""
        if self.connect_status:
            self.exit()
