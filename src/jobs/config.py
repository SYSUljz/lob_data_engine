import json
import os
from pathlib import Path
    #FIXME : vps data path should not define in config.json it makes managment complex

def load_vps_config(config_path="vps_config.json"):
    """
    Loads VPS configuration from a JSON file.
    Searches in the current directory, project root, or parent directories.
    """
    # Potential paths to check
    current_dir = Path(os.getcwd())
    search_paths = [
        current_dir / config_path,
        current_dir.parent / config_path,
        Path(__file__).resolve().parent.parent.parent / config_path # Project root relative to src/jobs/config.py
    ]

    config = {}
    
    for path in search_paths:
        if path.exists():
            try:
                with open(path, 'r') as f:
                    config = json.load(f)
                print(f"🔧 Loaded configuration from {path}")
                break
            except Exception as e:
                print(f"⚠️ Error reading config from {path}: {e}")
    
    # Return defaults if not found
    if not config:
        print(f"⚠️ Configuration file '{config_path}' not found. Using internal defaults.")
        config = {
            "remote_alias": "vps",
            "remote_base_path": "/root/lob_data_engine/perp_data"
        }
        
    return config
