import re
import functools
import asyncio
from aiohttp import web

import ciscosparkapi


async def dummy(*args, **kwargs):
    pass


class Server:
    def __init__(self, config, loop):
        self._loop = loop
        self._config = config
        self._id = None
        self._displayname = None
        self._api = ciscosparkapi.CiscoSparkAPI(access_token=config['token'])
        self._callbacks = []
        self._hooks = {}
        self._get_routes = {}
        self._post_routes = {}
        self._default_message = dummy
        self._pre_message = dummy
        self._on_startup = dummy
        self._on_room_created = dummy
        self._messages = []

    def listen(self, match, callback):
        self._callbacks.append((re.compile(match), callback))

    def default_message(self, callback):
        self._default_message = callback

    def pre_message(self, callback):
        self._pre_message = callback

    def on_startup(self, callback):
        self._on_startup = callback

    def roomcreation(self, callback):
        self._on_room_created = callback

    async def setup(self):
        await self.cleanup()
        await asyncio.wait([self._get_self(), self._register_webhooks()])
        await self._on_startup(self._api)
        return await self._setup_webserver()

    async def cleanup(self):
        await self._remove_webhooks()

    async def _handle_message(self, message):
        if message.id in self._messages:
            return

        self._messages.append(message.id)

        text = message.text

        await self._pre_message(self._loop, self._api, message)
        callbacks = [c for c in self._callbacks if c[0].match(text.lower())]
        if callbacks:
            await asyncio.wait(
                [callback[1](
                    self._api,
                    message)
                    for callback in callbacks]
            )
        else:
            await self._default_message(self._api, message)

    async def _message_created(self, webhook_data):
        if webhook_data['data']['personId'] == self._id:
            return

        message = await self._loop.run_in_executor(
            None,
            self._api.messages.get,
            webhook_data['data']['id'],
        )

        await self._handle_message(message)

    async def _room_created(self, webhook_data):
        if not webhook_data['data']['personId'] == self._id:
            return

        person = await self._loop.run_in_executor(
            None,
            self._api.people.get,
            webhook_data['actorId']
        )

        await self._on_room_created(
            self._api,
            webhook_data['data']['roomId'],
            webhook_data['data']['id'],
            person,
        )

    async def _webhook_notified(self, request):
        data = await request.json()
        name = data['name']
        if name in self._hooks.keys():
            await self._hooks[name](data)
        return web.Response()

    async def _setup_webserver(self):
        self._application = web.Application()
        self._application.router.add_post(
            '/',
            self._webhook_notified
        )

        for route, callback in self._get_routes.items():
            self._application.router.add_get(
                route,
                functools.partial(
                    self._handle_get,
                    callback,
                )
            )
        for route, callback in self._post_routes.items():
            self._application.router.add_post(
                route,
                functools.partial(
                    self._handle_post,
                    callback,
                )
            )

        self._handler = self._application.make_handler()
        server = await self._loop.create_server(
            self._handler,
            '127.0.0.1',
            self._config['port'],
        )
        return server

    async def _handle_get(self, callback, request):
        html, code = await callback(self._api, request)
        return web.Response(
            text=html,
            content_type='text/html',
            status=code,
        )

    async def _handle_post(self, callback, request):
        code = await callback(self._api, request)
        return web.Response(status=code)

    def add_get(self, route, callback):
        self._get_routes[route] = callback

    def add_post(self, route, callback):
        self._post_routes[route] = callback

    async def _get_self(self):
        me = await self._loop.run_in_executor(
            None,
            self._api.people.me,
        )
        self._id = me.id
        self._displayname = me.displayName.replace(' (bot)', '')

    async def _register_webhooks(self):
        if self._callbacks or self._default_message:
            await self._create_webhook(
                'message created',
                'messages',
                'created',
                self._message_created,
            )
        if self._on_room_created:
            await self._create_webhook(
                'room created',
                'memberships',
                'created',
                self._room_created,
            )

    async def _create_webhook(self, name, resource, event, callback):
        self._hooks[name] = callback
        await self._loop.run_in_executor(
            None,
            self._api.webhooks.create,
            name,
            self._config['webhook'],
            resource,
            event,
        )

    async def _remove_webhooks(self):
        hooks = await self._loop.run_in_executor(
            None,
            self._api.webhooks.list,
        )

        for hook in hooks:
            await self._loop.run_in_executor(
                None,
                self._api.webhooks.delete,
                hook.id
            )
