import asyncio
import logging
import os

from aiokafka import AIOKafkaConsumer, TopicPartition
from pydantic import BaseModel
from dispytch import EventListener
from dispytch.kafka import KafkaConsumer
from dispytch import Event, HandlerGroup


class User(BaseModel):
    id: str
    email: str
    name: str


class UserCreatedEvent(BaseModel):
    user: User
    timestamp: int


user_events = HandlerGroup()


@user_events.handler(topic='user_events', event='user_registered')
async def handle_user_registered(
        event: Event[UserCreatedEvent]
):
    user = event.body.user
    timestamp = event.body.timestamp

    logging.info(f"[User Registered] {user.id} - {user.email} at {timestamp}")


async def main():
    logging.basicConfig(level=logging.INFO)

    partition = int(os.getenv("KAFKA_PARTITION", "0"))
    kafka_consumer = AIOKafkaConsumer(bootstrap_servers='redpanda:9092',
                                      enable_auto_commit=False,
                                      group_id='consumer_group_id',
                                      auto_offset_reset='earliest')
    await kafka_consumer.start()

    tp = TopicPartition("user_events", partition)
    kafka_consumer.assign([tp])

    listener = EventListener(KafkaConsumer(kafka_consumer))
    listener.add_handler_group(user_events)

    await listener.listen()


if __name__ == '__main__':
    asyncio.run(main())
