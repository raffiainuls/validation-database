import os
import sys
import yaml
import json
import logging
from running_validation import main as run_validation

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CREDENTIALS_FOLDER = "creds"  # Folder tempat file kredensial disimpan

def load_config(config_path):
    """
    Load configuration from a YAML file.
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            logging.info(f"Successfully loaded configuration from {config_path}")
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as exc:
        logging.error(f"Error parsing YAML file: {exc}")
        sys.exit(1)

def load_all_credentials():
    """
    Load all credentials from JSON files in the creds folder.
    """
    credentials = {}
    if not os.path.exists(CREDENTIALS_FOLDER):
        logging.error(f"Credentials folder not found: {CREDENTIALS_FOLDER}")
        sys.exit(1)

    for file_name in os.listdir(CREDENTIALS_FOLDER):
        if file_name.endswith('.json'):
            file_path = os.path.join(CREDENTIALS_FOLDER, file_name)
            try:
                with open(file_path, 'r') as file:
                    credentials_name = os.path.splitext(file_name)[0]  # Use the file name (without extension) as the key
                    credentials[credentials_name] = json.load(file)
                    logging.info(f"Successfully loaded credentials from {file_path}")
            except json.JSONDecodeError as exc:
                logging.error(f"Error parsing JSON file {file_name}: {exc}")
                sys.exit(1)

    if not credentials:
        logging.error("No credentials found in the creds folder.")
        sys.exit(1)

    return credentials

def parse_args(argv):
    """Parse: python config.py <config_file> [--mode missing|full].

    --mode missing : only detect missing IDs between source and target.
    --mode full    : detect missing IDs AND compare values across all common
                     columns (default).
    """
    config_file = None
    mode = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == '--mode':
            if i + 1 >= len(argv):
                logging.error("--mode requires a value: missing | full")
                sys.exit(1)
            mode = argv[i + 1].lower()
            i += 2
        elif arg.startswith('--mode='):
            mode = arg.split('=', 1)[1].lower()
            i += 1
        elif config_file is None:
            config_file = arg
            i += 1
        else:
            logging.error(f"Unexpected argument: {arg}")
            sys.exit(1)

    if config_file is None:
        logging.error("Usage: python config.py <config_file> [--mode missing|full]")
        sys.exit(1)
    if mode is not None and mode not in ('missing', 'full'):
        logging.error(f"Invalid --mode '{mode}'. Use 'missing' or 'full'.")
        sys.exit(1)
    return config_file, mode


if __name__ == "__main__":
    config_file, mode = parse_args(sys.argv[1:])

    # Load configuration and all credentials
    config = load_config(config_file)
    credentials = load_all_credentials()

    # Merge credentials into the config dictionary
    config['credentials'] = credentials

    # CLI --mode overrides any 'mode' set in the YAML config.
    if mode is not None:
        config['mode'] = mode

    # Run the validation process with the merged configuration
    run_validation(config)
