import uvicorn
from fastapi import FastAPI
from app import routes
from config import config


app = FastAPI(title="File Converter App")
[app.include_router(router) for router in routes]


if __name__ == "__main__":
    uvicorn.run(
        "web:app",
        host="0.0.0.0",
        port=config.PORT,
        reload=False,
        workers=2,
    )
