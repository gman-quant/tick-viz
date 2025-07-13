# src/web/app_factory.py


from aiohttp import web
import config.config as config
from .websocket_handler import websocket_handler

async def init_app():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_static('/', path=str(config.OUTPUT_DIR.resolve()), name='static')
    return app
