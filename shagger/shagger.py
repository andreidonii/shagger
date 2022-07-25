import asyncio
import json
import traceback
from json import JSONDecodeError

from aiokafka import AIOKafkaConsumer

DEFAULT_GROUP = 'DEFAULT_GROUP'


class App(object):
    _should_exit = False
    __listeners = {}
    loop = None

    def __init__(self, function, *args, **kwargs):
        topic_parts = function.__qualname__.split('.')
        if len(topic_parts) == 1:
            topic_parts.insert(0, DEFAULT_GROUP)
        group, topic = topic_parts
        if group not in self.__listeners:
            self.__listeners[group] = {}
        self.__listeners[group][function.__qualname__] = (self, function)
        self.function = function
        print('HERE2', function, args, kwargs)

    def __call__(self, *args, **kwargs):
        print('HERE call', args, kwargs, self.function)
        self.function(*args, **kwargs)

    @classmethod
    async def __run_loop(cls, *, bootstrap_servers):
        consumers = {
            group: AIOKafkaConsumer(*cls.__listeners[group].keys(), bootstrap_servers=bootstrap_servers, group_id=group)
            for group in cls.__listeners.keys()}
        futures = [cls.__main_loop(c, g) for g, c in consumers.items()]
        await asyncio.gather(*futures)

    @classmethod
    def run(cls, *, bootstrap_servers):
        cls.loop = asyncio.get_event_loop()
        cls.loop.run_until_complete(cls.__run_loop(bootstrap_servers=bootstrap_servers))
        cls.loop.close()

    @classmethod
    async def __main_loop(cls, consumer, group):
        await consumer.start()
        while not cls._should_exit:
            print('loop', group)
            # App._should_exit = True
            try:
                # Consume messages
                async for msg in consumer:
                    print("consumed: ", msg.topic, msg.partition, msg.offset,
                          msg.key, msg.value, msg.timestamp)
                    c, f = cls.__listeners[group][msg.topic]
                    v = msg.value.decode('utf-8')
                    try:
                        j = json.loads(v)
                    except JSONDecodeError:
                        j = v
                    try:
                        if group == DEFAULT_GROUP:
                            f(j)
                        else:
                            f(c, j)
                    except BaseException as e:
                        print('Error', e, traceback.format_exc())
            finally:
                # Will leave consumer group; perform autocommit if enabled.
                await consumer.stop()


def service(function=None, *, topic=None):
    if function:
        return App(function)
    else:
        def wrapper(f):
            return App(f, topic)

        return wrapper


class SomeConsumer(App):
    @service(topic='asd')
    def function1(self, data):
        print('Hello-1', data, data.get('asd', None))


@service
def function2(data):
    print('Hello-2', data, data.__dict__.get('asd', None))


# args = ('asd',)
# function2(*args)
SomeConsumer.run(bootstrap_servers='localhost:9092')
