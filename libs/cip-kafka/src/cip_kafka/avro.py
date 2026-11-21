"""Confluent wire-format Avro serializer, deserializer, and Schema Registry client.

Confluent wire format (ADR-13):
- Byte 0: Magic byte 0x00
- Bytes 1-4: 4-byte big-endian unsigned integer (Schema ID)
- Bytes 5+: Binary Avro encoded payload
"""

import io
import json
import os
import struct
import threading
from pathlib import Path
from typing import Any, cast

import fastavro
import httpx


class SchemaRegistryClient:
    """Thread-safe Schema Registry client with local in-memory caching.

    Fails fast if base_url is missing or empty. For tests, instantiate explicitly via
    `SchemaRegistryClient.in_memory()`.
    """

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        if not base_url or not base_url.strip():
            raise ValueError(
                "SchemaRegistryClient requires a non-empty base_url. "
                "For testing without an external registry, use SchemaRegistryClient.in_memory()."
            )
        self.base_url: str | None = base_url.rstrip("/")
        self.timeout = timeout
        self._lock = threading.Lock()

        # In-memory caches: canonical_json -> id, id -> parsed_schema_dict
        self._id_by_schema: dict[str, int] = {}
        self._schema_by_id: dict[int, dict[str, Any]] = {}
        self._subject_versions: dict[str, list[int]] = {}
        self._next_id: int = 1

        self._http_client: httpx.Client | None = httpx.Client(base_url=self.base_url, timeout=self.timeout)

    @classmethod
    def in_memory(cls) -> "SchemaRegistryClient":
        """Explicit test helper creating an isolated in-memory registry client."""
        client = cls.__new__(cls)
        client.base_url = None
        client.timeout = 5.0
        client._lock = threading.Lock()
        client._id_by_schema = {}
        client._schema_by_id = {}
        client._subject_versions = {}
        client._next_id = 1
        client._http_client = None
        return client

    def close(self) -> None:
        if self._http_client is not None:
            self._http_client.close()
            self._http_client = None

    def _canonicalize(self, schema: dict[str, Any]) -> str:
        return json.dumps(schema, sort_keys=True)

    def register_schema(self, subject: str, schema: dict[str, Any]) -> int:
        """Register a schema under a subject, returning the schema ID."""
        canonical = self._canonicalize(schema)
        with self._lock:
            if canonical in self._id_by_schema:
                return self._id_by_schema[canonical]

        if self._http_client is not None:
            schema_str = json.dumps(schema)
            resp = self._http_client.post(
                f"/subjects/{subject}/versions",
                json={"schema": schema_str},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            if resp.status_code not in (200, 201):
                raise RuntimeError(
                    f"Schema Registry registration failed for subject '{subject}': {resp.status_code} {resp.text}"
                )
            schema_id = int(resp.json()["id"])
        else:
            with self._lock:
                schema_id = self._next_id
                self._next_id += 1

        with self._lock:
            self._id_by_schema[canonical] = schema_id
            self._schema_by_id[schema_id] = schema
            self._subject_versions.setdefault(subject, []).append(schema_id)

        return schema_id

    def get_schema_id(self, subject: str, schema: dict[str, Any]) -> int:
        """Lookup schema ID for an already registered schema. Does NOT auto-register."""
        canonical = self._canonicalize(schema)
        with self._lock:
            if canonical in self._id_by_schema:
                return self._id_by_schema[canonical]

        if self._http_client is not None:
            schema_str = json.dumps(schema)
            resp = self._http_client.post(
                f"/subjects/{subject}",
                json={"schema": schema_str},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            if resp.status_code == 200:
                schema_id = int(resp.json()["id"])
                with self._lock:
                    self._id_by_schema[canonical] = schema_id
                    self._schema_by_id[schema_id] = schema
                return schema_id
            raise KeyError(
                f"Schema for subject '{subject}' not registered in Schema Registry ({resp.status_code}): {resp.text}"
            )

        raise KeyError(f"Schema for subject '{subject}' not registered in in-memory registry")

    def get_schema(self, schema_id: int) -> dict[str, Any]:
        """Fetch schema definition by ID from cache or Schema Registry."""
        with self._lock:
            if schema_id in self._schema_by_id:
                return self._schema_by_id[schema_id]

        if self._http_client is not None:
            resp = self._http_client.get(
                f"/schemas/ids/{schema_id}",
                headers={"Accept": "application/vnd.schemaregistry.v1+json"},
            )
            if resp.status_code != 200:
                raise KeyError(f"Schema ID {schema_id} not found in Schema Registry ({resp.status_code}): {resp.text}")
            raw = resp.json()["schema"]
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            parsed_dict = cast(dict[str, Any], parsed)
            with self._lock:
                self._schema_by_id[schema_id] = parsed_dict
            return parsed_dict

        raise KeyError(f"Schema ID {schema_id} not found in in-memory registry")


class ConfluentAvroSerializer:
    """Serializes Python dicts into Confluent wire format (0x00 + 4-byte Schema ID + Avro payload)."""

    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema: dict[str, Any],
        subject: str | None = None,
        auto_register: bool | None = None,
    ) -> None:
        self.client = schema_registry_client
        self.raw_schema = schema
        self.parsed_schema = fastavro.parse_schema(schema)
        self.subject = subject

        # Default auto_register=False unless CIP_ENV=local (ADR-13)
        if auto_register is None:
            cip_env = os.environ.get("CIP_ENV", "").strip().lower()
            self.auto_register = cip_env == "local"
        else:
            self.auto_register = auto_register

        self._cached_schema_id: int | None = None

    def get_schema_id(self, topic: str) -> int:
        if self._cached_schema_id is not None:
            return self._cached_schema_id
        subject = self.subject or f"{topic}-value"
        if self.auto_register:
            sid = self.client.register_schema(subject, self.raw_schema)
        else:
            sid = self.client.get_schema_id(subject, self.raw_schema)
        self._cached_schema_id = sid
        return sid

    def encode(self, topic: str, record: dict[str, Any]) -> bytes:
        """Encode a record dictionary into Confluent wire-format bytes."""
        schema_id = self.get_schema_id(topic)
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        fastavro.schemaless_writer(buf, self.parsed_schema, record)
        return buf.getvalue()


class ConfluentAvroDeserializer:
    """Deserializes Confluent wire-format bytes using writer schema by ID and optional reader schema."""

    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        reader_schema: dict[str, Any] | None = None,
    ) -> None:
        self.client = schema_registry_client
        self.raw_reader_schema = reader_schema
        self.parsed_reader_schema = fastavro.parse_schema(reader_schema) if reader_schema is not None else None

    def decode(self, payload: bytes) -> dict[str, Any]:
        """Decode Confluent wire-format bytes into a record dictionary."""
        if len(payload) < 5:
            raise ValueError(f"Payload too short for Confluent Avro wire format: {len(payload)} bytes < 5")
        if payload[0] != 0:
            raise ValueError(f"Invalid Confluent wire format magic byte: {payload[0]:#04x} (expected 0x00)")

        schema_id = struct.unpack(">I", payload[1:5])[0]
        writer_schema_raw = self.client.get_schema(schema_id)
        writer_schema = fastavro.parse_schema(writer_schema_raw)

        buf = io.BytesIO(payload[5:])
        result = fastavro.schemaless_reader(
            buf,
            writer_schema=writer_schema,
            reader_schema=self.parsed_reader_schema,
        )
        return cast(dict[str, Any], result)


class ConfluentAvroCodec:
    """Convenience helper combining serializer and deserializer for a schema file or dict."""

    def __init__(
        self,
        schema_source: Path | str | dict[str, Any],
        schema_registry_client: SchemaRegistryClient,
        subject: str | None = None,
        auto_register: bool | None = None,
    ) -> None:
        if isinstance(schema_source, (str, Path)):
            with open(schema_source) as f:
                raw_schema = json.load(f)
        else:
            raw_schema = schema_source

        self.raw_schema: dict[str, Any] = raw_schema
        self.client = schema_registry_client
        self.serializer = ConfluentAvroSerializer(
            schema_registry_client=schema_registry_client,
            schema=raw_schema,
            subject=subject,
            auto_register=auto_register,
        )
        self.deserializer = ConfluentAvroDeserializer(
            schema_registry_client=schema_registry_client,
            reader_schema=raw_schema,
        )

    def encode(self, topic: str, record: dict[str, Any]) -> bytes:
        return self.serializer.encode(topic, record)

    def decode(self, payload: bytes) -> dict[str, Any]:
        return self.deserializer.decode(payload)
