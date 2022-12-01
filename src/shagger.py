import asyncio
import pickle
import traceback
from json import JSONDecodeError
from logging import getLogger
from typing import List, Coroutine

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import Config
from validation import Validate

logger = getLogger(__name__)
DEFAULT_GROUP = 'DEFAULT_GROUP'


class ShaggerBase(object):
    _should_exit = False
    _listeners = {}
    _publisher: AIOKafkaProducer = None
    # _init = None
    _bootstrap_servers: str = None
    loop = None

    @classmethod
    async def send(cls, topic: str, data: bytes):
        return await cls._publisher.send(topic=topic, value=data)


class App(ShaggerBase):
    __config: Config = None
    __init: Coroutine = None
    __validate: Validate = None

    def __init__(self, function, *args, **kwargs):
        topic_parts = function.__qualname__.split('.')
        if len(topic_parts) == 1:
            topic_parts.insert(0, DEFAULT_GROUP)
        group, topic = topic_parts
        if group not in ShaggerBase._listeners:
            ShaggerBase._listeners[group] = {}
        ShaggerBase._listeners[group][function.__qualname__] = (self, function)
        self.function = function

    @classmethod
    def run(cls, *, bootstrap_servers=None, init=None, config_file='../service.yaml'):
        cls.__config = Config(config_file)
        if cls.__config.validate:
            cls.__validate = Validate(cls.__config.validate)

        ShaggerBase._bootstrap_servers = bootstrap_servers or cls.__config.bootstrap_servers
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

                    # if cls.__validate is not None:
                    #     print(cls.__validate.check_in(j, '.'.join([group, msg.topic])))
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
            # except SchemaError:
            #     logger.error("Validation did not went through")
            finally:
                await consumer.stop()


def service(function=None):
    if function:
        return App(function)
    else:
        def wrapper(f):
            return App(f)

        return wrapper


class Stub(ShaggerBase):
    __config = None

    def __init__(self, function):
        topic_parts = function.__qualname__.split('.')
        if len(topic_parts) == 1:
            topic_parts.insert(0, DEFAULT_GROUP)
        self.group, self.topic = topic_parts
        self.function = function

    async def __call__(self, *args, **kwargs):
        logger.debug('method direct call' + str([args, kwargs, self.function]))
        # self.function(*args, **kwargs)
        if self.group == DEFAULT_GROUP:
            topic = self.topic
        else:
            topic = '.'.join([self.group, self.topic])

        if ShaggerBase._publisher is None:
            publisher = AIOKafkaProducer(
                bootstrap_servers=ShaggerBase._bootstrap_servers or Config().bootstrap_servers,
                compression_type='gzip', acks=0)
            await publisher.start()
            res = await publisher.send(topic=topic, value=pickle.dumps({"args": args, "kwargs": kwargs}))
            await publisher.stop()
            return res
        else:
            # print("Sending to", '.'.join([self.group, self.topic]))
            return await ShaggerBase._publisher.send(topic=topic,
                                                     value=pickle.dumps({"args": args, "kwargs": kwargs}))

    @classmethod
    def run(cls, *, bootstrap_servers=None, init=None, config_file='service.yaml'):
        cls.__config = Config(config_file)
        print('HERE2')

        if cls.__config['validate']:
            print('HERE')
            cls.__validate = Validate(cls.__config.validate)
        ShaggerBase._bootstrap_servers = bootstrap_servers
        cls.__init = init
        if ShaggerBase.loop is None:
            asyncio.get_event_loop().run_until_complete(init)


def stub(function=None):
    if function:
        return Stub(function)
    else:
        def wrapper(f):
            return Stub(f)

        return wrapper


async def send(*, group=DEFAULT_GROUP, function: str, data: bytes):
    return await ShaggerBase.send('.'.join([group, function]), data)
