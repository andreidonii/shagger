import asyncio
import json
from time import sleep

from aiokafka import AIOKafkaProducer

from shagger import stub, Stub, service, App


class SomeConsumer:
    @service
    async def function3(self, data):
        pass

    @stub
    async def function1(self, data):
        pass


@stub
async def function2():
    pass


async def test():
    # await send(group='SomeConsumer',function='function1', data=json.dumps({'asd': 'asdasdasd'}).encode('utf-8'))
    c = SomeConsumer()
    await function2({"asd": "yoho"})
    await c.function1({"asd": "yoho"}, asd="feas", test2="lasldal,sc,a")

asyncio.new_event_loop().run_until_complete(test())
# Stub.run(bootstrap_servers='localhost:9092', init=test())
