from lob_data_engine.manager import WSListenerManager
from lob_data_engine.binance_perp_listener import BinancePerpListener

def main():
    listeners = [
        BinancePerpListener(),
    ]
    
    manager = WSListenerManager(listeners)
    manager.run_forever()

if __name__ == "__main__":
    main()
