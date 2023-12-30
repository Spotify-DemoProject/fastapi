import os, sys

now_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(now_dir, "../lib")
log_dir = os.path.join(now_dir, "../log")

sys.path.append(lib_dir)

from log_libs import *
from os_libs import *

from fastapi import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
import logging
import traceback
from time import time


# class ErrorLoggerMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:

#         endpoint_path = request.url.path
#         logger_name = endpoint_path.replace("/", "_")[1:] + "_error"

#         log_file_dir = f"{log_dir}/error/{logger_name}"
#         check_mkdirs(log_file_dir) # module

#         logger = setup_logger(name=logger_name, level=logging.ERROR, log_dir=log_file_dir)
           
#         try:
#             response = await call_next(request)
#             remove_logger(logger) # module
#             return response
        
#         except Exception as e:
#             message = f"Internal Server Error - {endpoint_path} - {str(e)}"
#             logger.error(message)
#             send_line_notification_thread(message=message) # module
#             remove_logger(logger) # module
#             raise HTTPException(status_code=500, detail="Internal Server Error")


# class TimeLoggerMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:

#         endpoint_path = request.url.path
#         logger_name = endpoint_path.replace("/", "_")[1:] + "_info"

#         log_file_dir = f"{log_dir}/time/{logger_name}"
#         check_mkdirs(log_file_dir) # module

#         logger = setup_logger(name=logger_name, level=logging.INFO, log_dir=log_file_dir)
        
#         start_time = time()

#         try:
#             response = await call_next(request)
#             return response
        
#         except Exception as e:
#             raise HTTPException(status_code=500, detail="Internal Server Error")
        
#         finally:
#             end_time = time()
#             elapsed_time = end_time - start_time
#             message = f"Elapsed Time: {elapsed_time:.2f} seconds"
#             logger.info(message)
#             remove_logger(logger)  # module

class LoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:

        endpoint_path = request.url.path
        logger_name_error = endpoint_path.replace("/", "_")[1:] + "_error"
        logger_name_info = endpoint_path.replace("/", "_")[1:] + "_info"

        log_file_dir_error = f"{log_dir}/error/{logger_name_error}"
        log_file_dir_info = f"{log_dir}/info/{logger_name_info}"
        
        check_mkdirs(log_file_dir_error) # module
        check_mkdirs(log_file_dir_info) # module
        
        start_time = time()

        logger_error = setup_logger(name=logger_name_error, level=logging.ERROR, log_dir=log_file_dir_error)
        logger_info = setup_logger(name=logger_name_info, level=logging.INFO, log_dir=log_file_dir_info)
        
        try:   
            response = await call_next(request)
            return response
        
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_info = traceback.extract_tb(exc_traceback)[-1]
            filename = tb_info.filename
            lineno = tb_info.lineno
            line =tb_info.line
            message = f"Internal Server Error - {endpoint_path} - {filename} - {lineno} - {line} - {str(e)}"
            logger_error.error(message)
            send_line_notification_thread(message=message) # module
            raise HTTPException(status_code=500, detail="Internal Server Error")
        
        finally:            
            end_time = time()
            elapsed_time = end_time - start_time
            message = f"Elapsed Time: {elapsed_time:.2f} seconds"
            logger_info.info(message)
            remove_logger(logger_error) # module
            remove_logger(logger_info) # module
