from fastapi import FastAPI
from router import sql_routers, json_routers, csv_routers
# from middleware_c.logger_middleware import ErrorLoggerMiddleware, TimeLoggerMiddleware
from middleware_c.logger_middleware import LoggerMiddleware

app = FastAPI()
app.add_middleware(LoggerMiddleware)
# app.add_middleware(ErrorLoggerMiddleware)
# app.add_middleware(TimeLoggerMiddleware)
app.include_router(sql_routers.router)
app.include_router(json_routers.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
    