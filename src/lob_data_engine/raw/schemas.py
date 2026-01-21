from typing import TypedDict, List, Literal, Tuple, Union, Optional

# ==========================================
# Hyperliquid Processed Schemas (as written to Parquet)
# ==========================================

class HyperliquidProcessedSnapshot(TypedDict):
    """
    Processed L2 Book Snapshot for Hyperliquid.
    """
    coin: str
    channel: Literal["l2Book"]
    nSigFigs: Optional[int]
    exchange_time: int
    local_time: float
    bids_px: List[str]
    bids_sz: List[str]
    bids_n: List[int]
    asks_px: List[str]
    asks_sz: List[str]
    asks_n: List[int]

class HyperliquidProcessedTrade(TypedDict):
    """
    Processed Trade Record for Hyperliquid.
    """
    coin: str
    channel: Literal["trades"]
    exchange_time: int
    local_time: float
    side: str
    px: str
    sz: int
    hash: str
    tid: Optional[int]
    users: str  # JSON string of users

HyperliquidData = Union[HyperliquidProcessedSnapshot, HyperliquidProcessedTrade]

# ==========================================
# Binance Schemas (as written to Parquet)
# ==========================================

class BinanceTrade(TypedDict):
    """
    Trade Message for Binance.
    continus check : t = t.shift(1) + 1
    """
    e: Literal["trade"]
    E: int
    s: str
    t: int
    p: str
    q: str
    b: int
    a: int
    T: int
    m: bool
    M: bool
    local_time: Optional[float]

class BinancePerpTrade(TypedDict):
    """
    Trade Message for Binance Perpetual Futures.
    continus check : t = t.shift(1) + 1
    """
    e: Literal["trade"]
    E: int
    s: str
    t: int
    p: str
    q: str
    T: int
    m: bool
    local_time: Optional[float]

class BinanceDiff(TypedDict):
    """
    Depth Update (Diff) for Binance.
    """
    e: Literal["depthUpdate"]
    E: int
    s: str
    U: int
    u: int
    pu: int
    b: List[List[str]]
    a: List[List[str]]
    local_time: Optional[float]

class BinanceSnapshot(TypedDict):
    """
    Enriched Depth Snapshot for Binance.
    """
    e: Literal["depthSnapshot"]
    E: int
    s: str
    lastUpdateId: int
    bids: List[List[str]]
    asks: List[List[str]]
    local_time: float
    is_revalidation: bool

class BinanceForceOrder(TypedDict):
    """
    Force Liquidation Order for Binance Perpetual Futures.
    """
    e: Literal["forceOrder"]
    E: int
    o: dict  # Nested order details
    local_time: Optional[float]
    s: Optional[str] # Extracted symbol for convenience

class BinancePartialDepth(TypedDict):
    """
    Partial Book Depth for Binance (e.g. depth20).
    continus check : pu = u.shift(1) 
    """
    lastUpdateId: int
    bids: List[List[str]]
    asks: List[List[str]]
    local_time: Optional[float]
    s: str
    e: Literal["depthPartial"]
    E: Optional[int]
    U: Optional[int]
    u: Optional[int]
    pu: Optional[int]

BinanceData = Union[BinanceTrade, BinancePerpTrade, BinanceDiff, BinanceSnapshot, BinanceForceOrder, BinancePartialDepth]