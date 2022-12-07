import asyncio
import os
import pickle
import traceback
from json import JSONDecodeError
from logging import getLogger
from typing import Coroutine, List

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from shagger.base import ShaggerBase, DEFAULT_GROUP

logger = getLogger(__name__)


class Service(ShaggerBase):
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
                        result = None
                        if group == DEFAULT_GROUP:
                            if "args" in j and "kwargs" in j:
                                result = f(*j["args"], **j["kwargs"])
                            else:
                                result = f(j)
                        else:
                            if "args" in j and "kwargs" in j:
                                result = f(c, *j["args"], **j["kwargs"])
                            else:
                                result = f(c, j)

                        if msg.headers:
                            return_topic = [hdr[1] for hdr in msg.headers if hdr[0] == 'return_topic'][0]
                            if return_topic:
                                await ShaggerBase._publisher.send(return_topic, value=pickle.dumps(result))
                    except BaseException as e:
                        logger.error('Error' + str([e, traceback.format_exc()]))
            finally:
                await consumer.stop()
