import pytest

TOPIC = "topic"
PAYLOAD = b"hello world"
GROUP_ID = "group_id"


def test_sync(kafka_broker):
    """Demonstrate using the kafka_broker fixture in an ordinary test."""
    with kafka_broker.producer() as producer:
        producer.produce(TOPIC, PAYLOAD)

    with kafka_broker.consumer(
        {"group.id": GROUP_ID, "auto.offset.reset": "earliest"}
    ) as consumer:
        consumer.subscribe([TOPIC])
        (message,) = consumer.consume()
        assert message.value() == PAYLOAD

    with kafka_broker.admin() as admin:
        assert TOPIC in admin.list_topics().topics


@pytest.mark.asyncio
async def test_async(kafka_broker):
    """Demonstrate using the kafka_broker fixture in an async test."""
    async with kafka_broker.aio_producer() as producer:
        await producer.produce(TOPIC, PAYLOAD)

    async with kafka_broker.aio_consumer(
        {"group.id": GROUP_ID, "auto.offset.reset": "earliest"}
    ) as consumer:
        await consumer.subscribe([TOPIC])
        (message,) = await consumer.consume()
    assert message.value() == PAYLOAD

    with kafka_broker.admin() as admin:
        assert TOPIC in admin.list_topics().topics
