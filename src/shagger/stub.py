import asyncio
import os
import pickle
import uuid
from logging import getLogger

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from shagger.base import ShaggerBase, DEFAULT_GROUP

logger = getLogger(__name__)


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
        headers = [('return_topic', return_channel.encode())]
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
            return await self.__send(topic=topic, value=pickle.dumps({"args": args, "kwargs": kwargs}),
                                     publisher=ShaggerBase._publisher)
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
