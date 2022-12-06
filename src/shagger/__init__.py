import asyncio
import os
import pickle
import traceback
import uuid
from json import JSONDecodeError
from logging import getLogger
from typing import List, Coroutine

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = getLogger(__name__)
DEFAULT_GROUP = 'DEFAULT_GROUP'


class ShaggerBase(object):
    _should_exit = False
    _listeners = {}
    _publisher: AIOKafkaProducer = None
    _bootstrap_servers: str = None
    loop = None

    @classmethod
    async def send(cls, topic: str, data: bytes):
        return await cls._publisher.send(topic=topic, value=data)


class App(ShaggerBase):
    __init: Coroutine = None

    def __init__(self, function, topic=None):
        topic_parts = (topic or function.__qualname__).split('.')
        if len(topic_parts) == 1:
            topic_parts.insert(0, DEFAULT_GROUP)
        group, topic = topic_parts
        if group not in ShaggerBase._listeners:
            ShaggerBase._listeners[group] = {}
        ShaggerBase._listeners[group][function.__qualname__] = (self, function)
        self.function = function

    @classmethod
    def run(cls, *, bootstrap_servers=None, init=None):
        ShaggerBase._bootstrap_servers = bootstrap_servers or os.environ["bootstrap_servers"]
        cls.__init = init
        ShaggerBase.loop = asyncio.get_event_loop()
        ShaggerBase.loop.run_until_complete(cls.__run_loop())

    def __call__(self, *args, **kwargs):
        logger.debug('method direct call' + str([args, kwargs, self.function]))
        self.function(*args, **kwargs)

    @classmethod
    async def __run_loop(cls):
        ShaggerBase._publisher = AIOKafkaProducer(bootstrap_servers=ShaggerBase._bootstrap_servers,
                                                  compression_type='gzip', acks=0)
        await ShaggerBase._publisher.start()
        consumers = {
            group: AIOKafkaConsumer(*ShaggerBase._listeners[group].keys(),
                                    bootstrap_servers=ShaggerBase._bootstrap_servers,
                                    # group_id=group
                                    )
            for group in ShaggerBase._listeners.keys()}
        futures: List[Coroutine] = [cls.__main_loop(c, g) for g, c in consumers.items()]
        if cls.__init:
            futures.append(cls.__init)
        await asyncio.gather(*futures)
        await ShaggerBase._publisher.stop()

    @classmethod
    async def __main_loop(cls, consumer: AIOKafkaConsumer, group: str):
        await consumer.start()
        while not cls._should_exit:
            try:
                async for msg in consumer:
                    logger.debug("consumed message: ", msg.topic, msg.partition, msg.offset,
                                 msg.key, msg.value, msg.timestamp)
                    c, f = ShaggerBase._listeners[group][msg.topic]

                    try:
                        j = pickle.loads(msg.value)
                    except JSONDecodeError:
                        j = msg.value

                    try:
                        if group == DEFAULT_GROUP:
                            if "args" in j and "kwargs" in j:
                                f(*j["args"], **j["kwargs"])
                            else:
                                f(j)
                        else:
                            if "args" in j and "kwargs" in j:
                                f(c, *j["args"], **j["kwargs"])
                            else:
                                f(c, j)
                    except BaseException as e:
                        logger.error('Error' + str([e, traceback.format_exc()]))
            finally:
                await consumer.stop()


def service(function=None, topic=None):
    if function:
        return App(function, topic=topic)
    else:
        def wrapper(f):
            return App(f, topic=topic)

        return wrapper


class Stub(ShaggerBase):

    def __init__(self, function, topic=None, wait_result=False):
        self.wait_result = wait_result
        topic_parts = (topic or function.__qualname__).split('.')
        if len(topic_parts) == 1:
            topic_parts.insert(0, DEFAULT_GROUP)
        self.group, self.topic = topic_parts
        self.function = function

    async def __send(self, *, publisher: AIOKafkaProducer, topic: str, value: bytes):
        return_channel = uuid.uuid4().__str__()
        if not self.wait_result:
            return await publisher.send(topic=topic, value=value)
        headers = [('return_channel', return_channel.encode())]
        consumer = AIOKafkaConsumer([return_channel],
                                    bootstrap_servers=ShaggerBase._bootstrap_servers
                                    )
        await consumer.start()
        try:
            await publisher.send(topic=topic, value=value, headers=headers)
            async for msg in consumer:
                return pickle.loads(msg.value)
        finally:
            await consumer.stop()

    async def __call__(self, *args, **kwargs):
        logger.debug('method direct call' + str([args, kwargs, self.function]))
        if self.group == DEFAULT_GROUP:
            topic = self.topic
        else:
            topic = '.'.join([self.group, self.topic])

        if ShaggerBase._publisher is not None:
            return await self.__send(topic=topic,value=pickle.dumps({"args": args, "kwargs": kwargs}), publisher=ShaggerBase._publisher)
        publisher = AIOKafkaProducer(
            bootstrap_servers=ShaggerBase._bootstrap_servers or os.environ["bootstrap_servers"],
            compression_type='gzip')
        await publisher.start()
        res = await self.__send(topic=topic, value=pickle.dumps({"args": args, "kwargs": kwargs}), publisher=publisher)
        await publisher.stop()
        return res

    @classmethod
    def run(cls, *, bootstrap_servers=None, init=None):
        ShaggerBase._bootstrap_servers = bootstrap_servers
        cls.__init = init
        if ShaggerBase.loop is None:
            asyncio.get_event_loop().run_until_complete(init)


def stub(function=None, topic=None, wait_result=False):
    if function:
        return Stub(function, topic=topic, wait_result=wait_result)
    else:
        def wrapper(f):
            return Stub(f, topic=topic, wait_result=wait_result)

        return wrapper


async def send(*, group=DEFAULT_GROUP, function: str, data: bytes, topic=None):
    return await ShaggerBase.send(topic or '.'.join([group, function]), data)
