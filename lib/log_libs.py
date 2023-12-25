import os, sys

now_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(now_dir, "../lib")
sys.path.append(lib_dir)

from os_libs import *

def file_handler(log_dir:str):
    import logging
    from datetime import datetime
    
    log_format = "%(asctime)s - %(levelname)s - %(message)s"

    file_handler = logging.FileHandler(
        f"{log_dir}/{datetime.now().strftime('%Y-%m-%d_%H')}.log", 
        encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(log_format))
    
    return file_handler

def setup_logger(name:str, level=None, log_dir=None):
    import logging

    logger = logging.getLogger(name)
    
    if level == None:
        logger.setLevel(logging.DEBUG)
    else:
        try:
            logger.setLevel(level)
        except:
            print("CANNOT SET LOG LEVEL - WRONG 'level' PARAMS")
            return

    if log_dir != None:
        handler = file_handler(log_dir)
        logger.addHandler(handler)

    return logger

def remove_logger(logger):
    try:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        for filter_ in logger.filters[:]:
            logger.removeFilter(filter_)
    except Exception as E:
        error_message = f"ERROR APPEARED - {E}"
        logger.error(error_message)
        print(error_message)
