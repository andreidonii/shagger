import asyncio
import os
import pickle
import traceback
import uuid
from json import JSONDecodeError
from logging import getLogger
from typing import List, Coroutine

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shagger.base import ShaggerBase
from shagger.service import Service
from shagger.stub import Stub

logger = getLogger(__name__)





def service(function=None, topic=None):
    if function:
        return Service(function, topic=topic)
    else:
        def wrapper(f):
            return Service(f, topic=topic)

        return wrapper


def stub(function=None, topic=None, wait_result=False):
    if function:
        return Stub(function, topic=topic, wait_result=wait_result)
    else:
        def wrapper(f):
            return Stub(f, topic=topic, wait_result=wait_result)

        return wrapper


async def send(*, group=DEFAULT_GROUP, function: str, data: bytes, topic=None):
    return await ShaggerBase.send(topic or '.'.join([group, function]), data)
