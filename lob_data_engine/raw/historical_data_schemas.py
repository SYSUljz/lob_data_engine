from typing import TypedDict

class BinanceAggTrade(TypedDict):
    """
    Aggregated Trade from Binance Vision (CSV format).
    Standard columns from https://data.binance.vision headers (often no header, but standard order):
    agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
    """
    agg_trade_id: int
    price: str
    quantity: str
    first_trade_id: int
    last_trade_id: int
    transact_time: int
    is_buyer_maker: bool
