from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any

import pytest

pytest.importorskip("vnpy_femas.api", reason="缺少 Femas 原生扩展")

from vnpy.event import Event, EventEngine  # noqa: E402
from vnpy.trader.constant import (  # noqa: E402
    Direction,
    Exchange,
    Offset,
    OptionType,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.event import EVENT_TIMER  # noqa: E402
from vnpy.trader.object import (  # noqa: E402
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
from vnpy_femas.api import (  # noqa: E402
    USTP_FTDC_AF_Delete,
    USTP_FTDC_CAS_Accepted,
    USTP_FTDC_CAS_Rejected,
    USTP_FTDC_CAS_Submitted,
    USTP_FTDC_CHF_Speculation,
    USTP_FTDC_D_Buy,
    USTP_FTDC_D_Sell,
    USTP_FTDC_FCR_NotForceClose,
    USTP_FTDC_OF_CloseToday,
    USTP_FTDC_OF_CloseYesterday,
    USTP_FTDC_OF_Open,
    USTP_FTDC_OPT_AnyPrice,
    USTP_FTDC_OPT_LimitPrice,
    USTP_FTDC_OS_AllTraded,
    USTP_FTDC_OS_Canceled,
    USTP_FTDC_OS_NoTradeQueueing,
    USTP_FTDC_OS_PartTradedNotQueueing,
    USTP_FTDC_OS_PartTradedQueueing,
    USTP_FTDC_OT_CallOptions,
    USTP_FTDC_OT_NotOptions,
    USTP_FTDC_TC_GFD,
    USTP_FTDC_TC_IOC,
    USTP_FTDC_VC_AV,
    USTP_FTDC_VC_CV,
)
from vnpy_femas.gateway import femas_gateway  # noqa: E402
from vnpy_femas.gateway.femas_gateway import (  # noqa: E402
    CHINA_TZ,
    STATUS_FEMAS2VT,
    FemasGateway,
    FemasMdApi,
    FemasTdApi,
)


class Sink:
    def __init__(self) -> None:
        self.logs: list[str] = []
        self.ticks: list[TickData] = []
        self.contracts: list[ContractData] = []
        self.orders: list[OrderData] = []
        self.trades: list[TradeData] = []
        self.positions: list[PositionData] = []
        self.accounts: list[AccountData] = []

    def attach(self, gateway: FemasGateway) -> None:
        gateway.write_log = self.logs.append  # type: ignore[method-assign]
        gateway.on_tick = self.ticks.append  # type: ignore[method-assign]
        gateway.on_contract = self.contracts.append  # type: ignore[method-assign]
        gateway.on_order = self.orders.append  # type: ignore[method-assign]
        gateway.on_trade = self.trades.append  # type: ignore[method-assign]
        gateway.on_position = self.positions.append  # type: ignore[method-assign]
        gateway.on_account = self.accounts.append  # type: ignore[method-assign]


class CallRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def patch(self, monkeypatch: pytest.MonkeyPatch, api: object, names: list[str]) -> None:
        for name in names:
            monkeypatch.setattr(api, name, self.make_stub(name))

    def make_stub(self, name: str) -> Callable[..., int]:
        def stub(*args: Any) -> int:
            self.calls.append((name, args[0] if args else None))
            return 0
        return stub

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


TD_METHODS: list[str] = [
    "createFtdcTraderApi",
    "subscribePrivateTopic",
    "subscribePublicTopic",
    "subscribeUserTopic",
    "registerFront",
    "init",
    "exit",
    "reqDSUserCertification",
    "reqUserLogin",
    "reqQryUserInvestor",
    "reqQryInstrument",
    "reqOrderInsert",
    "reqOrderAction",
    "reqQryInvestorAccount",
    "reqQryInvestorPosition",
]

MD_METHODS: list[str] = [
    "createFtdcMdApi",
    "subscribeMarketDataTopic",
    "registerFront",
    "init",
    "exit",
    "reqUserLogin",
    "subMarketData",
]


@pytest.fixture(autouse=True)
def clear_contracts() -> Iterator[None]:
    femas_gateway.symbol_contract_map.clear()
    yield
    femas_gateway.symbol_contract_map.clear()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(femas_gateway, "sleep", lambda _seconds: None)


@pytest.fixture
def sink() -> Sink:
    return Sink()


@pytest.fixture
def recorder() -> CallRecorder:
    return CallRecorder()


@pytest.fixture
def gateway(sink: Sink, recorder: CallRecorder, monkeypatch: pytest.MonkeyPatch) -> FemasGateway:
    engine: EventEngine = EventEngine()
    gateway: FemasGateway = FemasGateway(engine, "FEMAS")
    sink.attach(gateway)
    recorder.patch(monkeypatch, gateway.td_api, TD_METHODS)
    recorder.patch(monkeypatch, gateway.md_api, MD_METHODS)
    return gateway


@pytest.fixture
def td_api(gateway: FemasGateway) -> FemasTdApi:
    return gateway.td_api


@pytest.fixture
def md_api(gateway: FemasGateway) -> FemasMdApi:
    return gateway.md_api


def add_contract(
    symbol: str = "rb2510",
    exchange: Exchange = Exchange.SHFE,
    size: int = 10,
) -> ContractData:
    contract: ContractData = ContractData(
        symbol=symbol,
        exchange=exchange,
        name=symbol,
        product=Product.FUTURES,
        size=size,
        pricetick=1,
        gateway_name="FEMAS",
    )
    femas_gateway.symbol_contract_map[symbol] = contract
    return contract


def order_request(
    order_type: OrderType = OrderType.LIMIT,
    offset: Offset = Offset.OPEN,
) -> OrderRequest:
    return OrderRequest(
        symbol="rb2510",
        exchange=Exchange.SHFE,
        direction=Direction.LONG,
        type=order_type,
        volume=2,
        price=3000,
        offset=offset,
    )


def instrument_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "OptionsType": USTP_FTDC_OT_NotOptions,
        "InstrumentID_2": "",
        "InstrumentID": "rb2510",
        "ExchangeID": "SHFE",
        "InstrumentName": "螺纹钢",
        "VolumeMultiple": 10,
        "PriceTick": 1,
    }
    data.update(overrides)
    return data


def rtn_order(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "InsertDate": "20251010",
        "InsertTime": "09:30:00",
        "InstrumentID": "rb2510",
        "ExchangeID": "SHFE",
        "UserOrderLocalID": "000001000007",
        "Direction": USTP_FTDC_D_Buy,
        "OffsetFlag": USTP_FTDC_OF_Open,
        "LimitPrice": 3000,
        "Volume": 2,
        "VolumeTraded": 0,
        "OrderStatus": USTP_FTDC_OS_NoTradeQueueing,
    }
    data.update(overrides)
    return data


def position_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "InstrumentID": "rb2510",
        "Direction": USTP_FTDC_D_Buy,
        "YdPosition": 1,
        "Position": 2,
        "PositionCost": 6000,
        "FrozenPosition": 1,
    }
    data.update(overrides)
    return data


def test_connect_prefixes_bare_address(gateway: FemasGateway, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}
    monkeypatch.setattr(gateway.td_api, "connect", lambda *args: seen.__setitem__("td", args[0]))
    monkeypatch.setattr(gateway.md_api, "connect", lambda *args: seen.__setitem__("md", args[0]))

    setting: dict[str, str | int | float | bool] = dict(FemasGateway.default_setting)
    setting["交易服务器"] = "127.0.0.1:17001"
    setting["行情服务器"] = "tcp://127.0.0.1:17002"
    gateway.connect(setting)

    assert seen["td"] == "tcp://127.0.0.1:17001"
    assert seen["md"] == "tcp://127.0.0.1:17002"


def test_timer_skips_first_tick_then_rotates_queries(gateway: FemasGateway) -> None:
    calls: list[str] = []

    def query_account() -> None:
        calls.append("account")

    def query_position() -> None:
        calls.append("position")

    gateway.query_account = query_account  # type: ignore[method-assign]
    gateway.query_position = query_position  # type: ignore[method-assign]
    gateway.init_query()
    event: Event = Event(EVENT_TIMER)

    gateway.process_timer_event(event)
    assert calls == []

    gateway.process_timer_event(event)
    gateway.process_timer_event(event)
    gateway.process_timer_event(event)

    assert calls == ["account", "position"]


def test_front_connected_without_auth_logs_in(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.auth_code = ""
    td_api.userid = "u1"
    td_api.password = "p1"
    td_api.brokerid = "9999"
    td_api.onFrontConnected()

    assert recorder.names() == ["reqUserLogin"]
    request: dict[str, Any] = recorder.calls[0][1]
    assert request["UserID"] == "u1"
    assert request["BrokerID"] == "9999"


def test_front_connected_with_auth_authenticates(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.auth_code = "AUTH"
    td_api.appid = "app"
    td_api.onFrontConnected()

    assert recorder.names() == ["reqDSUserCertification"]
    request: dict[str, Any] = recorder.calls[0][1]
    assert request["AuthCode"] == "AUTH"
    assert request["AppID"] == "app"
    assert request["EncryptType"] == "1"


def test_auth_success_logs_in(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.onRspDSUserCertification({}, {"ErrorID": 0, "ErrorMsg": ""}, 1, True)

    assert recorder.names() == ["reqUserLogin"]


def test_login_adopts_max_local_id(td_api: FemasTdApi, sink: Sink, recorder: CallRecorder) -> None:
    td_api.userid = "u1"
    td_api.brokerid = "9999"
    td_api.onRspUserLogin(
        {"MaxOrderLocalID": "2000000"},
        {"ErrorID": 0, "ErrorMsg": ""},
        1,
        True,
    )

    assert td_api.localid == 2000000
    assert td_api.login_status is True
    assert recorder.calls == [("reqQryUserInvestor", {"BrokerID": "9999", "UserID": "u1"})]
    assert "交易服务器登录成功" in sink.logs


def test_login_failed_does_not_send_again(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.onRspUserLogin({}, {"ErrorID": 1, "ErrorMsg": "bad"}, 1, True)
    assert td_api.login_failed is True

    td_api.login()

    assert recorder.calls == []


def test_investor_query_requests_instrument(td_api: FemasTdApi, sink: Sink, recorder: CallRecorder) -> None:
    td_api.onRspQryUserInvestor({"InvestorID": "inv1"}, {}, 1, True)

    assert td_api.investorid == "inv1"
    assert recorder.names() == ["reqQryInstrument"]
    assert sink.logs == ["投资者代码查询成功"]


def test_send_order_requires_offset(td_api: FemasTdApi, sink: Sink, recorder: CallRecorder) -> None:
    assert td_api.send_order(order_request(offset=Offset.NONE)) == ""
    assert recorder.names() == []
    assert sink.logs == ["请选择开平方向"]


def test_send_order_returns_padded_local_id(td_api: FemasTdApi, sink: Sink, recorder: CallRecorder) -> None:
    td_api.userid = "u1"
    td_api.brokerid = "9999"
    td_api.investorid = "inv1"

    vt_orderid: str = td_api.send_order(order_request(offset=Offset.CLOSETODAY))

    # localid 初值是 int(10e5 + 8888)，发出前先加一，再左补零到 12 位。
    assert vt_orderid == "FEMAS.000001008889"
    request: dict[str, Any] = recorder.calls[0][1]
    assert request["UserOrderLocalID"] == "000001008889"
    assert request["ExchangeID"] == "SHFE"
    assert request["Volume"] == 2
    assert request["OrderPriceType"] == USTP_FTDC_OPT_LimitPrice
    assert request["Direction"] == USTP_FTDC_D_Buy
    assert request["TimeCondition"] == USTP_FTDC_TC_GFD
    assert request["VolumeCondition"] == USTP_FTDC_VC_AV
    assert request["HedgeFlag"] == USTP_FTDC_CHF_Speculation
    assert request["ForceCloseReason"] == USTP_FTDC_FCR_NotForceClose
    # CLOSETODAY 目前映射到平昨常量。
    assert request["OffsetFlag"] == USTP_FTDC_OF_CloseYesterday
    assert sink.orders[0].status == Status.SUBMITTING


def test_send_order_market_uses_any_price(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.send_order(order_request(order_type=OrderType.MARKET))

    request: dict[str, Any] = recorder.calls[0][1]
    assert request["OrderPriceType"] == USTP_FTDC_OPT_AnyPrice
    assert request["TimeCondition"] == USTP_FTDC_TC_GFD


@pytest.mark.parametrize(
    ("order_type", "volume_condition"),
    [
        (OrderType.FAK, USTP_FTDC_VC_AV),
        (OrderType.FOK, USTP_FTDC_VC_CV),
    ],
)
def test_send_order_fak_and_fok(
    td_api: FemasTdApi,
    recorder: CallRecorder,
    order_type: OrderType,
    volume_condition: str,
) -> None:
    td_api.send_order(order_request(order_type=order_type, offset=Offset.CLOSEYESTERDAY))

    request: dict[str, Any] = recorder.calls[0][1]
    assert request["OrderPriceType"] == USTP_FTDC_OPT_LimitPrice
    assert request["TimeCondition"] == USTP_FTDC_TC_IOC
    assert request["VolumeCondition"] == volume_condition
    # CLOSEYESTERDAY 目前映射到平今常量。
    assert request["OffsetFlag"] == USTP_FTDC_OF_CloseToday


def test_send_order_stop_is_still_sent(td_api: FemasTdApi, sink: Sink, recorder: CallRecorder) -> None:
    vt_orderid: str = td_api.send_order(order_request(order_type=OrderType.STOP))

    assert vt_orderid == "FEMAS.000001008889"
    assert recorder.calls[0][1]["OrderPriceType"] == ""
    assert sink.orders[0].status == Status.SUBMITTING


def test_cancel_order_uses_new_action_id(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.userid = "u1"
    td_api.brokerid = "9999"
    td_api.investorid = "inv1"
    td_api.cancel_order(CancelRequest(orderid="000001000007", symbol="rb2510", exchange=Exchange.SHFE))

    request: dict[str, Any] = recorder.calls[0][1]
    assert request["UserOrderLocalID"] == "000001000007"
    assert request["UserOrderActionLocalID"] == "000001008889"
    assert request["ExchangeID"] == "SHFE"
    assert request["ActionFlag"] == USTP_FTDC_AF_Delete


def test_instrument_futures_cached(td_api: FemasTdApi, sink: Sink) -> None:
    td_api.onRspQryInstrument(instrument_data(ExchangeID="INE", InstrumentID="sc2510"), {}, 1, True)

    contract: ContractData = sink.contracts[0]
    assert contract.symbol == "sc2510"
    assert contract.exchange == Exchange.INE
    assert contract.product == Product.FUTURES
    assert contract.size == 10
    assert femas_gateway.symbol_contract_map["sc2510"] is contract
    assert sink.logs == ["合约信息查询成功"]


def test_czce_option_portfolio_drops_suffix(td_api: FemasTdApi, sink: Sink) -> None:
    data: dict[str, Any] = instrument_data(
        OptionsType=USTP_FTDC_OT_CallOptions,
        InstrumentID="SR501C5000",
        ExchangeID="CZCE",
        ProductID="SR501C",
        UnderlyingInstrID="SR501",
        StrikePrice=5000,
        ExpireDate="20250115",
    )
    td_api.onRspQryInstrument(data, {}, 1, False)

    contract: ContractData = sink.contracts[0]
    assert contract.product == Product.OPTION
    assert contract.option_portfolio == "SR501"
    assert contract.option_underlying == "SR501"
    assert contract.option_type == OptionType.CALL
    assert contract.option_strike == 5000
    assert contract.option_index == "5000"
    assert contract.option_expiry == datetime(2025, 1, 15)


def test_shfe_option_portfolio_keeps_product_id(td_api: FemasTdApi, sink: Sink) -> None:
    data: dict[str, Any] = instrument_data(
        OptionsType=USTP_FTDC_OT_CallOptions,
        InstrumentID="rb2510C3000",
        ExchangeID="SHFE",
        ProductID="rb_o",
        UnderlyingInstrID="rb2510",
        StrikePrice=3000,
        ExpireDate="20250915",
    )
    td_api.onRspQryInstrument(data, {}, 1, False)

    assert sink.contracts[0].option_portfolio == "rb_o"


def test_spread_uses_second_instrument(td_api: FemasTdApi, sink: Sink) -> None:
    td_api.onRspQryInstrument(instrument_data(InstrumentID_2="rb2601"), {}, 1, False)

    assert sink.contracts[0].product == Product.SPREAD


def test_status_codes_collide() -> None:
    # 撤单动作状态和委托状态共用字符，dict 只留下后写入的委托状态。
    assert USTP_FTDC_CAS_Submitted == USTP_FTDC_OS_PartTradedQueueing
    assert STATUS_FEMAS2VT[USTP_FTDC_CAS_Submitted] == Status.PARTTRADED
    assert USTP_FTDC_CAS_Rejected == USTP_FTDC_OS_NoTradeQueueing
    assert STATUS_FEMAS2VT[USTP_FTDC_CAS_Rejected] == Status.NOTTRADED
    assert USTP_FTDC_CAS_Accepted == USTP_FTDC_OS_PartTradedNotQueueing
    assert STATUS_FEMAS2VT[USTP_FTDC_CAS_Accepted] == Status.SUBMITTING


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (USTP_FTDC_OS_NoTradeQueueing, Status.NOTTRADED),
        (USTP_FTDC_OS_PartTradedQueueing, Status.PARTTRADED),
        (USTP_FTDC_OS_AllTraded, Status.ALLTRADED),
        (USTP_FTDC_OS_Canceled, Status.CANCELLED),
        (USTP_FTDC_CAS_Accepted, Status.SUBMITTING),
    ],
)
def test_order_maps_local_id_and_status(
    td_api: FemasTdApi,
    sink: Sink,
    status: str,
    expected: Status,
) -> None:
    td_api.onRtnOrder(rtn_order(
        OrderStatus=status,
        VolumeTraded=1,
        UserOrderLocalID="000002000000",
    ))

    order: OrderData = sink.orders[0]
    assert order.orderid == "000002000000"
    assert order.status == expected
    assert order.direction == Direction.LONG
    assert order.offset == Offset.OPEN
    assert order.traded == 1
    assert order.datetime == datetime(2025, 10, 10, 9, 30, tzinfo=CHINA_TZ)
    assert td_api.localid == 2000000


def test_order_close_today_flag_maps_to_yesterday(td_api: FemasTdApi, sink: Sink) -> None:
    td_api.onRtnOrder(rtn_order(OffsetFlag=USTP_FTDC_OF_CloseToday, Direction=USTP_FTDC_D_Sell))

    assert sink.orders[0].offset == Offset.CLOSEYESTERDAY
    assert sink.orders[0].direction == Direction.SHORT


def test_insert_error_marks_rejected(td_api: FemasTdApi, sink: Sink) -> None:
    add_contract()
    td_api.onRspOrderInsert(
        {
            "UserOrderLocalID": "000001000007",
            "InstrumentID": "rb2510",
            "Direction": USTP_FTDC_D_Buy,
            "OffsetFlag": USTP_FTDC_OF_Open,
            "LimitPrice": 3000,
            "Volume": 1,
        },
        {"ErrorID": 1, "ErrorMsg": "bad"},
        1,
        True,
    )

    assert sink.orders[0].status == Status.REJECTED
    assert sink.orders[0].orderid == "000001000007"
    assert "交易委托失败" in sink.logs[0]


def test_insert_without_error_is_ignored(td_api: FemasTdApi, sink: Sink) -> None:
    td_api.onRspOrderInsert({}, {"ErrorID": 0, "ErrorMsg": ""}, 1, True)

    assert sink.orders == []


def test_duplicate_trade_is_ignored(td_api: FemasTdApi, sink: Sink) -> None:
    data: dict[str, Any] = {
        "TradeID": "T1",
        "TradeDate": "20251010",
        "TradeTime": "09:31:00",
        "InstrumentID": "rb2510",
        "ExchangeID": "SHFE",
        "UserOrderLocalID": "000001000007",
        "Direction": USTP_FTDC_D_Buy,
        "OffsetFlag": USTP_FTDC_OF_Open,
        "TradePrice": 3001,
        "TradeVolume": 1,
    }
    td_api.onRtnTrade(data)
    td_api.onRtnTrade(data)

    trade: TradeData = sink.trades[0]
    assert len(sink.trades) == 1
    assert trade.tradeid == "T1"
    assert trade.orderid == "000001000007"
    assert trade.datetime == datetime(2025, 10, 10, 9, 31, tzinfo=CHINA_TZ)


def test_position_volume_price_and_frozen(td_api: FemasTdApi, sink: Sink) -> None:
    add_contract()
    td_api.onRspQryInvestorPosition(position_data(), {}, 1, False)
    assert sink.positions == []

    td_api.onRspQryInvestorPosition(
        position_data(Direction=USTP_FTDC_D_Sell, Position=4, PositionCost=8000, YdPosition=3, FrozenPosition=2),
        {},
        1,
        True,
    )

    assert len(sink.positions) == 2
    long_position: PositionData = sink.positions[0]
    short_position: PositionData = sink.positions[1]
    assert long_position.direction == Direction.LONG
    assert long_position.volume == 2
    assert long_position.yd_volume == 1
    assert long_position.price == 3000
    assert long_position.frozen == 1
    assert short_position.direction == Direction.SHORT
    assert short_position.volume == 4
    assert short_position.price == 2000
    assert td_api.positions == {}


def test_empty_position_tail_keeps_cache(td_api: FemasTdApi, sink: Sink) -> None:
    add_contract()
    td_api.onRspQryInvestorPosition(position_data(), {}, 1, False)

    td_api.onRspQryInvestorPosition({}, {}, 1, True)

    assert sink.positions == []
    assert len(td_api.positions) == 1


def test_position_without_contract_is_skipped(td_api: FemasTdApi, sink: Sink) -> None:
    td_api.onRspQryInvestorPosition(position_data(), {}, 1, True)

    assert sink.positions == []
    assert td_api.positions == {}


def test_account_uses_pre_balance_and_occupied_margin(td_api: FemasTdApi, sink: Sink) -> None:
    td_api.onRspQryInvestorAccount(
        {
            "AccountID": "A1",
            "PreBalance": 1000,
            "LongMargin": 10,
            "ShortMargin": 20,
            "Available": 800,
            "FrozenMargin": 5,
            "FrozenFee": 1,
            "FrozenPremium": 1,
            "DynamicRights": 1100,
        },
        {},
        1,
        True,
    )

    account: AccountData = sink.accounts[0]
    assert account.accountid == "A1"
    assert account.balance == 1000
    assert account.frozen == 30
    assert account.available == 970


def test_query_account_waits_for_investor(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.query_account()

    assert recorder.calls == []


def test_query_position_waits_for_contracts(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    td_api.investorid = "inv1"
    td_api.query_position()

    assert recorder.calls == []


def test_query_position_sends_investor(td_api: FemasTdApi, recorder: CallRecorder) -> None:
    add_contract()
    td_api.userid = "u1"
    td_api.brokerid = "9999"
    td_api.investorid = "inv1"
    td_api.query_position()

    assert recorder.calls == [(
        "reqQryInvestorPosition",
        {"BrokerID": "9999", "InvestorID": "inv1", "UserID": "u1"},
    )]


def test_depth_uses_trading_day(md_api: FemasMdApi, sink: Sink) -> None:
    add_contract()
    md_api.onRtnDepthMarketData({
        "InstrumentID": "rb2510",
        "TradingDay": "20251010",
        "UpdateTime": "09:30:00",
        "UpdateMillisec": 500,
        "Volume": 10,
        "LastPrice": 3000,
        "UpperLimitPrice": 3300,
        "LowerLimitPrice": 2700,
        "OpenPrice": 2990,
        "HighestPrice": 3010,
        "LowestPrice": 2980,
        "PreClosePrice": 2985,
        "BidPrice1": 2999,
        "AskPrice1": 3001,
        "BidVolume1": 5,
        "AskVolume1": 6,
    })

    tick: TickData = sink.ticks[0]
    assert tick.datetime == datetime(2025, 10, 10, 9, 30, 0, 500000, tzinfo=CHINA_TZ)
    assert tick.exchange == Exchange.SHFE
    assert tick.last_price == 3000
    assert tick.bid_price_1 == 2999
    assert tick.ask_volume_1 == 6


def test_depth_without_contract_is_ignored(md_api: FemasMdApi, sink: Sink) -> None:
    md_api.onRtnDepthMarketData({"InstrumentID": "rb2510"})

    assert sink.ticks == []


def test_subscribe_before_login_is_deferred(md_api: FemasMdApi, recorder: CallRecorder) -> None:
    md_api.subscribe(SubscribeRequest(symbol="rb2510", exchange=Exchange.SHFE))
    assert recorder.calls == []
    assert "rb2510" in md_api.subscribed

    md_api.onRspUserLogin({}, {"ErrorID": 0, "ErrorMsg": ""}, 1, True)

    assert recorder.calls == [("subMarketData", "rb2510")]


def test_md_front_connected_logs_in(md_api: FemasMdApi, recorder: CallRecorder) -> None:
    md_api.userid = "u1"
    md_api.password = "p1"
    md_api.brokerid = "9999"
    md_api.onFrontConnected()

    assert recorder.names() == ["reqUserLogin"]
    assert recorder.calls[0][1]["UserID"] == "u1"


def test_close_without_connection_does_not_exit(
    td_api: FemasTdApi,
    md_api: FemasMdApi,
    recorder: CallRecorder,
) -> None:
    td_api.close()
    md_api.close()

    assert recorder.calls == []
