import logging
import os

def get_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()

    # Optionally, write logs to a file
    log_file = os.path.join("logs", "log.txt")
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(log_file)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    return logger
