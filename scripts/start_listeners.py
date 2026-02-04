from lob_data_engine.manager import WSListenerManager
from lob_data_engine.binance_listener import BinanceListener
from lob_data_engine.hyperliquid_listener import HyperliquidListener

def main():
    listeners = [
        BinanceListener(),
        HyperliquidListener()
    ]
    
    manager = WSListenerManager(listeners)
    manager.run_forever()

if __name__ == "__main__":
    main()
