from __future__ import annotations
from typing import TypedDict, List, Literal, Union, Any

# Generic WebSocket Message
WsMsg = TypedDict("WsMsg", {
    "e": str, # Event type
    "E": int, # Event time
    "s": str, # Symbol
    # ... other fields vary
}, total=False)

# Trade Message (Event type "trade")
TradeMsg = TypedDict("TradeMsg", {
    "e": Literal["trade"],
    "E": int, # Event time
    "s": str, # Symbol
    "t": int, # Trade ID
    "p": str, # Price
    "q": str, # Quantity
    "T": int, # Trade time
    "m": bool, # Is the buyer the market maker?
})

# Force Liquidation Order Message
ForceOrderMsg = TypedDict("ForceOrderMsg", {
    "e": Literal["forceOrder"],
    "E": int, # Event time
    "o": Any, # Order details
})

# Depth (Partial) Message doesn't have "e" usually if purely snapshot stream,
# but usually comes as just data.
# However, if using <symbol>@depth20@100ms, payload is:
# { "lastUpdateId": ..., "bids": [...], "asks": [...] }
# It doesn't have "e" or "s" inside the payload for single streams usually?
# Wait, for Combined streams (wss://stream.binance.com:9443/stream?streams=...),
# the message is { "stream": "...", "data": {...} }.
# For raw /ws connection and subscribe, the message format for depth20 might just be the data.
# But how do we know which symbol?
# For Raw stream /ws, you subscribe. The messages come in.
# Partial Depth Payload:
# {
#   "lastUpdateId": 160,
#   "bids": [ [ "0.0024", "10" ] ],
#   "asks": [ [ "0.0026", "100" ] ]
# }
# It does NOT contain the symbol `s`.
# This is a problem if we multiplex multiple symbols on one socket without `stream` wrapper.
# If we use Combined Streams (`/stream`), we get:
# { "stream": "btcusdt@depth20@100ms", "data": { ... } }
# This is much safer for multiplexing.
# I should use Combined Streams or handle the lack of symbol in raw stream (which implies one socket per symbol or mapping).
# Actually, for raw streams, if I subscribe to multiple, I can't distinguish them easily if the payload doesn't have symbol.
# Trade streams HAVE symbol `s`.
# Diff Depth streams HAVE symbol `s`.
# Partial Depth streams DO NOT have symbol in the payload (according to some docs).
# Let's double check.
# If Partial Depth doesn't have symbol, I MUST use Combined Streams URL logic.
# URL: wss://stream.binance.com:9443/stream
# Params: channels in URL? Or subscribe?
# You can subscribe to combined streams too.
#
# I'll use Combined Streams mode generally as it's cleaner.
# URL: wss://stream.binance.com:9443/stream?streams=<stream1>/<stream2>/...
# OR connect to /stream and send SUBSCRIBE with proper method.

CombinedStreamMsg = TypedDict("CombinedStreamMsg", {
    "stream": str,
    "data": Any
})

DepthLevel = List[str] # [price, quantity]
PartialDepthData = TypedDict("PartialDepthData", {
    "lastUpdateId": int,
    "bids": List[DepthLevel],
    "asks": List[DepthLevel]
})

DepthUpdateMsg = TypedDict("DepthUpdateMsg", {
    "e": Literal["depthUpdate"],
    "E": int,
    "s": str,
    "U": int,
    "u": int,
    "pu": int,
    "b": List[DepthLevel],
    "a": List[DepthLevel]
})

