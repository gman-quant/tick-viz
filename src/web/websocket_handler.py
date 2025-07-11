# src/web/websocket_handler.py
from aiohttp import web
import asyncio

clients = set()

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    print("📡 WebSocket client connected")
    try:
        async for msg in ws:
            pass
    finally:
        clients.remove(ws)
        print("📡 WebSocket client disconnected")
    return ws

async def notify_clients():
    if clients:
        await asyncio.gather(*[ws.send_str("reload") for ws in clients])
