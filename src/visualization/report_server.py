# src/visualization/report_server.py

import asyncio

clients = set()

async def notify_clients():
    """通知所有連線的前端：報表已更新"""
    if clients:
        await asyncio.gather(*[client.send("reload") for client in clients])

async def handler(websocket):
    clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        clients.remove(websocket)

