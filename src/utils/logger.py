import logging
import os
from datetime import datetime


def setup_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_default_logger(module_name: str) -> logging.Logger:
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = "/opt/logs/training"
    log_file = f"{log_dir}/{module_name}_{timestamp}.log"
    
    return setup_logger(module_name, log_file)
