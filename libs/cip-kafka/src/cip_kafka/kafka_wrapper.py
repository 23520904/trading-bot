"""cip-kafka: Production-ready Kafka Producer and Consumer wrappers.
Enforces CIP conventions:
- Producer: acks=all, enable.idempotence=true, compression.type=zstd, linger.ms=5, header injection
- Consumer: enable.auto.commit=false, manual commit after sink success, DLQ routing on failures
"""

import logging
from collections.abc import Callable

from cip_core import DeadLetter
from confluent_kafka import Consumer, KafkaError, Message, Producer

logger = logging.getLogger("cip.kafka")


class CIPKafkaProducer:
    """Kafka producer enforcing CIP architectural invariants."""

    def __init__(self, bootstrap_servers: str, client_id: str):
        self.conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 5,
            "max.in.flight.requests.per.connection": 5,
        }
        self.producer = Producer(self.conf)

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: dict[str, str] | None = None,
        on_delivery: Callable[[KafkaError | None, Message], None] | None = None,
    ) -> None:
        raw_headers: list[tuple[str, str | bytes | None]] | None = None
        if headers:
            raw_headers = []
            for k, v in headers.items():
                raw_headers.append((k, v.encode("utf-8") if isinstance(v, str) else v))

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            headers=raw_headers,
            on_delivery=on_delivery,
        )
        self.producer.poll(0)

    def produce_dlq(
        self,
        dlq_topic: str,
        original_key: str,
        dead_letter: DeadLetter,
        encoded_dlq_payload: bytes,
    ) -> None:
        """Route poison pill or invalid message to Dead Letter Queue."""
        headers = {
            "error_code": dead_letter.error_code,
            "error_msg": dead_letter.error_msg,
            "src_topic": dead_letter.src_topic,
            "src_partition": str(dead_letter.src_partition),
            "src_offset": str(dead_letter.src_offset),
            "failed_at": str(dead_letter.failed_at),
        }
        self.produce(
            topic=dlq_topic,
            key=original_key,
            value=encoded_dlq_payload,
            headers=headers,
        )

    def flush(self, timeout: float = 10.0) -> int:
        return self.producer.flush(timeout)


class CIPKafkaConsumer:
    """Kafka consumer enforcing manual commit, sticky assignment, and at-least-once semantics."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
    ):
        self.conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": auto_offset_reset,
            "partition.assignment.strategy": "cooperative-sticky",
            "max.poll.interval.ms": 300000,
        }
        self.consumer = Consumer(self.conf)
        self.topics = topics
        self.consumer.subscribe(topics)

    def poll(self, timeout: float = 1.0) -> Message | None:
        return self.consumer.poll(timeout)

    def commit(self, message: Message | None = None, asynchronous: bool = False) -> None:
        """Commit offsets after sink or downstream pipeline succeeded."""
        if message is not None:
            if asynchronous:
                self.consumer.commit(message=message, asynchronous=True)
            else:
                self.consumer.commit(message=message, asynchronous=False)
        else:
            if asynchronous:
                self.consumer.commit(asynchronous=True)
            else:
                self.consumer.commit(asynchronous=False)

    def close(self) -> None:
        self.consumer.close()
