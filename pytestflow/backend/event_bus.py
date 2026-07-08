# event_bus.py
import asyncio
from collections import defaultdict

class EventBus:
    def __init__(self):
        self._subscribers = defaultdict(list)
        self._queue = asyncio.Queue()

    def set_loop(self, loop):
        self.loop = loop

    def on(self, cmd_name, callback):
        self._subscribers[cmd_name].append(callback)

    async def emit(self, cmd_name, data=None):
        await self._queue.put((cmd_name, data))

    async def start(self):
        while True:
            cmd_name, data = await self._queue.get()
            for callback in self._subscribers[cmd_name]:
                asyncio.create_task(callback(data))


event_bus = EventBus()
pending_requests = {}
