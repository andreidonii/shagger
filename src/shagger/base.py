from aiokafka import AIOKafkaProducer


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

