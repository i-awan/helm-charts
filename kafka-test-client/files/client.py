#!/usr/bin/env python3
"""Kafka test client: produce / consume / roundtrip with schema-conformant data.

Modes (first CLI argument):
  sample     print one generated record and exit (no Kafka needed; good for checking a schema)
  check      preflight: parse schema, reach Kafka (+ Schema Registry if enabled)
  produce    generate records that conform to the schema and send them
  consume    read records, decode them and check they conform
  roundtrip  produce N records then consume them back from a fresh group (used by `helm test`)

Everything is configured through environment variables (see the chart's templates).
The schema file is mounted at SCHEMA_PATH; SCHEMA_FORMAT is "avro" or "jsonschema".
"""
import datetime
import decimal
import io
import json
import logging
import os
import random
import re
import signal
import string
import sys
import time
import uuid

import fastavro
import jsonschema
from jsonschema import validators as js_validators

log = logging.getLogger("ktc")


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
class ConfigError(Exception):
    pass


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise ConfigError(f"missing required environment variable {name}")
    return value


def env_bool(name, default=False):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def env_float(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def as_json(obj):
    return json.dumps(obj, default=str, sort_keys=True)


def short(text, limit=600):
    return text if len(text) <= limit else text[:limit] + f"...(+{len(text) - limit} chars)"


# --------------------------------------------------------------------------
# schema loading
# --------------------------------------------------------------------------
class Schema:
    """Holds the schema text plus whatever parsed form each format needs."""

    def __init__(self, fmt, text):
        self.fmt = fmt
        self.text = text
        try:
            self.raw = json.loads(text)
        except ValueError as exc:
            raise ConfigError(f"schema is not valid JSON: {exc}")
        if fmt == "avro":
            self.named = {}
            self.parsed = fastavro.parse_schema(self.raw, named_schemas=self.named)
        elif fmt == "jsonschema":
            cls = js_validators.validator_for(self.raw)
            cls.check_schema(self.raw)
            self.validator = cls(self.raw, format_checker=cls.FORMAT_CHECKER)
        else:
            raise ConfigError(f"SCHEMA_FORMAT must be 'avro' or 'jsonschema', got {fmt!r}")

    @classmethod
    def load(cls):
        path = env("SCHEMA_PATH", "/schema/schema")
        fmt = env("SCHEMA_FORMAT", "avro").strip().lower()
        with open(path, "r", encoding="utf-8") as handle:
            return cls(fmt, handle.read())

    def validate(self, record):
        """Raise ValueError if the record does not conform."""
        if self.fmt == "avro":
            try:
                fastavro.validate(record, self.parsed, raise_errors=True)
            except Exception as exc:  # fastavro raises ValidationError with details
                raise ValueError(str(exc))
        else:
            errors = sorted(self.validator.iter_errors(record), key=lambda e: list(e.path))
            if errors:
                first = errors[0]
                where = "/".join(str(p) for p in first.path) or "<root>"
                raise ValueError(f"{where}: {first.message}")


# --------------------------------------------------------------------------
# Avro data generator
# --------------------------------------------------------------------------
class AvroGenerator:
    PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}

    def __init__(self, schema, rnd, max_depth=6):
        self.schema = schema
        self.rnd = rnd
        self.max_depth = max_depth

    def record(self):
        return self.gen(self.schema.parsed, 0)

    # -- dispatch ----------------------------------------------------------
    def gen(self, s, depth):
        if isinstance(s, list):
            return self.union(s, depth)
        if isinstance(s, str):
            if s in self.schema.named:
                return self.gen(self.schema.named[s], depth)
            return self.primitive(s, {})
        t = s["type"]
        if isinstance(t, (list, dict)):
            return self.gen(t, depth)
        if t in self.PRIMITIVES:
            return self.primitive(t, s)
        if t == "record" or t == "error":
            return {f["name"]: self.gen(f["type"], depth + 1) for f in s["fields"]}
        if t == "enum":
            return self.rnd.choice(s["symbols"])
        if t == "array":
            if depth > self.max_depth:
                return []
            return [self.gen(s["items"], depth + 1) for _ in range(self.rnd.randint(0, 3))]
        if t == "map":
            if depth > self.max_depth:
                return {}
            return {self.word(3, 8): self.gen(s["values"], depth + 1) for _ in range(self.rnd.randint(0, 3))}
        if t == "fixed":
            return self.fixed(s)
        if t in self.schema.named:
            return self.gen(self.schema.named[t], depth)
        raise ConfigError(f"unsupported Avro type: {t!r}")

    def union(self, branches, depth):
        nonnull = [b for b in branches if b != "null"]
        if "null" in branches and (not nonnull or depth > self.max_depth or self.rnd.random() < 0.15):
            return None
        branch = self.rnd.choice(nonnull)
        value = self.gen(branch, depth + 1)
        # fastavro needs (record_name, value) when a union holds several records
        names = [self.record_name(b) for b in nonnull]
        if sum(1 for n in names if n) > 1:
            name = self.record_name(branch)
            if name:
                return (name, value)
        return value

    def record_name(self, branch):
        node = self.schema.named.get(branch) if isinstance(branch, str) else branch
        if isinstance(node, dict) and node.get("type") in ("record", "error"):
            return node["name"]
        return None

    # -- leaves ------------------------------------------------------------
    def word(self, lo=5, hi=12):
        return "".join(self.rnd.choices(string.ascii_lowercase, k=self.rnd.randint(lo, hi)))

    def fixed(self, s):
        if s.get("logicalType") == "decimal":
            return self.decimal_value(s)
        return bytes(self.rnd.getrandbits(8) for _ in range(s["size"]))

    def decimal_value(self, s):
        precision = int(s.get("precision", 9))
        scale = int(s.get("scale", 0))
        digits = max(1, min(precision, 9))
        unscaled = self.rnd.randint(0, 10 ** digits - 1)
        return decimal.Decimal(unscaled).scaleb(-scale).quantize(decimal.Decimal(1).scaleb(-scale))

    def now(self, tz=True):
        base = datetime.datetime.now(datetime.timezone.utc if tz else None)
        return base - datetime.timedelta(seconds=self.rnd.randint(0, 30 * 86400))

    def primitive(self, t, s):
        logical = s.get("logicalType")
        r = self.rnd
        if t == "null":
            return None
        if t == "boolean":
            return r.random() < 0.5
        if t == "int":
            if logical == "date":
                return self.now().date()
            if logical == "time-millis":
                return datetime.time(r.randint(0, 23), r.randint(0, 59), r.randint(0, 59))
            return r.randint(0, 10000)
        if t == "long":
            if logical in ("timestamp-millis", "timestamp-micros"):
                return self.now()
            if logical in ("local-timestamp-millis", "local-timestamp-micros"):
                return self.now(tz=False)
            if logical == "time-micros":
                return datetime.time(r.randint(0, 23), r.randint(0, 59), r.randint(0, 59))
            return r.randint(0, 10 ** 9)
        if t in ("float", "double"):
            return round(r.uniform(0, 1000), 3)
        if t == "bytes":
            if logical == "decimal":
                return self.decimal_value(s)
            return bytes(r.getrandbits(8) for _ in range(r.randint(4, 16)))
        if t == "string":
            if logical == "uuid":
                return str(uuid.UUID(int=r.getrandbits(128), version=4))
            return self.word()
        raise ConfigError(f"unsupported Avro primitive: {t!r}")


# --------------------------------------------------------------------------
# JSON Schema data generator
# --------------------------------------------------------------------------
class JsonGenerator:
    """Generates data for the common subset of JSON Schema.

    Supported: type, enum, const, properties/required, items, minItems/maxItems,
    uniqueItems, min/maxLength, pattern (needs `exrex`), format (date-time, date,
    time, email, uri, hostname, ipv4, uuid), minimum/maximum/exclusive*, multipleOf,
    allOf/anyOf/oneOf, local $ref (#/$defs/.. or #/definitions/..).
    The result is validated against the schema; generation is retried on mismatch.
    """

    ATTEMPTS = 30

    def __init__(self, schema, rnd, max_depth=6):
        self.schema = schema
        self.root = schema.raw
        self.rnd = rnd
        self.max_depth = max_depth

    def record(self):
        last = None
        for _ in range(self.ATTEMPTS):
            value = self.gen(self.root, 0)
            try:
                self.schema.validate(value)
                return value
            except ValueError as exc:
                last = exc
        raise ConfigError(
            f"could not generate data matching the JSON schema after {self.ATTEMPTS} attempts "
            f"(last error: {last}). The schema probably uses a keyword this generator does not support."
        )

    # -- helpers -----------------------------------------------------------
    def deref(self, ref):
        if not ref.startswith("#"):
            raise ConfigError(f"only local $ref is supported, got {ref!r}")
        node = self.root
        for part in ref[1:].split("/"):
            if part == "":
                continue
            node = node[part.replace("~1", "/").replace("~0", "~")]
        return node

    def resolve(self, s):
        hops = 0
        while isinstance(s, dict) and "$ref" in s:
            hops += 1
            if hops > 25:
                raise ConfigError("$ref chain too deep")
            target = self.deref(s["$ref"])
            rest = {k: v for k, v in s.items() if k != "$ref"}
            s = {**target, **rest} if isinstance(target, dict) else target
        return s

    def merge(self, schemas):
        out = {}
        for part in schemas:
            part = self.resolve(part)
            if not isinstance(part, dict):
                continue
            for key, value in part.items():
                if key == "properties":
                    out.setdefault("properties", {}).update(value)
                elif key == "required":
                    out["required"] = sorted(set(out.get("required", [])) | set(value))
                else:
                    out[key] = value
        return out

    def word(self, lo, hi):
        return "".join(self.rnd.choices(string.ascii_lowercase, k=self.rnd.randint(lo, hi)))

    # -- main dispatch -----------------------------------------------------
    def gen(self, s, depth):
        s = self.resolve(s)
        if s is True or s == {}:
            return self.word(4, 10)
        if s is False:
            raise ConfigError("schema 'false' cannot be generated")
        if "const" in s:
            return s["const"]
        if "enum" in s:
            return self.rnd.choice(s["enum"])
        if "allOf" in s:
            base = {k: v for k, v in s.items() if k != "allOf"}
            return self.gen(self.merge([base] + list(s["allOf"])), depth)
        for key in ("oneOf", "anyOf"):
            if key in s:
                base = {k: v for k, v in s.items() if k != key}
                branch = self.rnd.choice(s[key])
                return self.gen(self.merge([base, branch]), depth)

        t = s.get("type")
        if isinstance(t, list):
            options = [x for x in t if x != "null"] or t
            t = self.rnd.choice(options)
        if t is None:
            if "properties" in s:
                t = "object"
            elif "items" in s:
                t = "array"
            else:
                t = "string"

        if t == "object":
            return self.obj(s, depth)
        if t == "array":
            return self.array(s, depth)
        if t == "string":
            return self.string(s)
        if t == "integer":
            return self.number(s, integer=True)
        if t == "number":
            return self.number(s, integer=False)
        if t == "boolean":
            return self.rnd.random() < 0.5
        if t == "null":
            return None
        raise ConfigError(f"unsupported JSON schema type: {t!r}")

    def obj(self, s, depth):
        props = s.get("properties", {})
        required = set(s.get("required", []))
        out = {}
        for name, sub in props.items():
            if name in required or (depth <= self.max_depth and self.rnd.random() < 0.85):
                out[name] = self.gen(sub, depth + 1)
        return out

    def array(self, s, depth):
        lo = int(s.get("minItems", 0))
        hi = int(s.get("maxItems", lo + 3))
        hi = max(lo, min(hi, lo + 3))
        if depth > self.max_depth:
            hi = lo
        count = self.rnd.randint(lo, hi)
        items = s.get("items", {})
        out = []
        attempts = 0
        while len(out) < count and attempts < count * 12 + 12:
            attempts += 1
            candidate = self.gen(items, depth + 1)
            if s.get("uniqueItems") and candidate in out:
                continue
            out.append(candidate)
        return out

    def string(self, s):
        fmt = s.get("format")
        r = self.rnd
        if fmt == "date-time":
            when = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=r.randint(0, 30 * 86400))
            return when.strftime("%Y-%m-%dT%H:%M:%S.") + f"{when.microsecond // 1000:03d}Z"
        if fmt == "date":
            return (datetime.date.today() - datetime.timedelta(days=r.randint(0, 365))).isoformat()
        if fmt == "time":
            return f"{r.randint(0, 23):02d}:{r.randint(0, 59):02d}:{r.randint(0, 59):02d}Z"
        if fmt == "email":
            return f"{self.word(4, 8)}@example.com"
        if fmt in ("uri", "url"):
            return f"https://example.com/{self.word(4, 8)}"
        if fmt == "hostname":
            return f"{self.word(4, 8)}.example.com"
        if fmt == "ipv4":
            return ".".join(str(r.randint(1, 254)) for _ in range(4))
        if fmt == "uuid":
            return str(uuid.UUID(int=r.getrandbits(128), version=4))
        pattern = s.get("pattern")
        if pattern:
            try:
                import exrex
            except ImportError:
                raise ConfigError("schema uses 'pattern' but the 'exrex' package is not installed")
            return exrex.getone(pattern)
        lo = int(s.get("minLength", 5))
        hi = int(s.get("maxLength", max(lo, 12)))
        lo, hi = min(lo, hi), max(lo, hi)
        return self.word(lo, hi)

    def number(self, s, integer):
        r = self.rnd
        lo = s.get("minimum")
        hi = s.get("maximum")
        if "exclusiveMinimum" in s:
            lo = s["exclusiveMinimum"] + (1 if integer else 0.001)
        if "exclusiveMaximum" in s:
            hi = s["exclusiveMaximum"] - (1 if integer else 0.001)
        if lo is None and hi is None:
            lo, hi = 0, 10000
        elif lo is None:
            lo = hi - 10000
        elif hi is None:
            hi = lo + 10000
        multiple = s.get("multipleOf")
        if multiple:
            first = int(-(-lo // multiple))   # ceil
            last = int(hi // multiple)
            if last < first:
                raise ConfigError("multipleOf does not fit between minimum and maximum")
            value = r.randint(first, last) * multiple
            return int(value) if integer else value
        if integer:
            return r.randint(int(-(-lo // 1)), int(hi // 1))
        return round(r.uniform(lo, hi), 3)


def make_generator(schema, rnd):
    return AvroGenerator(schema, rnd) if schema.fmt == "avro" else JsonGenerator(schema, rnd)


# --------------------------------------------------------------------------
# Kafka / Schema Registry configuration
# --------------------------------------------------------------------------
def kafka_conf():
    conf = {
        "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", required=True),
        "security.protocol": env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper(),
    }
    proto = conf["security.protocol"]
    if "SSL" in proto:
        ca = env("KAFKA_SSL_CA")
        cert = env("KAFKA_SSL_CERT")
        key = env("KAFKA_SSL_KEY")
        if ca:
            conf["ssl.ca.location"] = ca
        if cert and key:
            conf["ssl.certificate.location"] = cert
            conf["ssl.key.location"] = key
        conf["ssl.endpoint.identification.algorithm"] = env("KAFKA_SSL_ENDPOINT_ID", "https")
    if "SASL" in proto:
        mech = env("KAFKA_SASL_MECHANISM", required=True)
        conf["sasl.mechanism"] = mech
        if mech in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            conf["sasl.username"] = env("KAFKA_SASL_USERNAME", required=True)
            conf["sasl.password"] = env("KAFKA_SASL_PASSWORD", required=True)
        elif mech == "OAUTHBEARER":
            conf["sasl.oauthbearer.method"] = "oidc"
            conf["sasl.oauthbearer.client.id"] = env("KAFKA_OAUTH_CLIENT_ID", required=True)
            conf["sasl.oauthbearer.client.secret"] = env("KAFKA_OAUTH_CLIENT_SECRET", required=True)
            conf["sasl.oauthbearer.token.endpoint.url"] = env("KAFKA_OAUTH_TOKEN_URL", required=True)
            scope = env("KAFKA_OAUTH_SCOPE")
            if scope:
                conf["sasl.oauthbearer.scope"] = scope
        else:
            raise ConfigError(f"unsupported SASL mechanism {mech!r}")
    extra = env("KAFKA_EXTRA_PROPS", "{}")
    conf.update(json.loads(extra))
    return conf


def sr_client():
    from confluent_kafka.schema_registry import SchemaRegistryClient

    conf = {"url": env("SR_URL", required=True)}
    if env("SR_SSL_CA"):
        conf["ssl.ca.location"] = env("SR_SSL_CA")
    if env("SR_SSL_CERT") and env("SR_SSL_KEY"):
        conf["ssl.certificate.location"] = env("SR_SSL_CERT")
        conf["ssl.key.location"] = env("SR_SSL_KEY")
    if env("SR_BASIC_AUTH"):
        conf["basic.auth.user.info"] = env("SR_BASIC_AUTH")
    return SchemaRegistryClient(conf)


# --------------------------------------------------------------------------
# serialisation: with or without Schema Registry
# --------------------------------------------------------------------------
class Codec:
    """encode(record, topic) -> bytes ; decode(bytes, topic) -> record. Both validate."""

    def __init__(self, schema):
        from confluent_kafka.serialization import MessageField, SerializationContext

        self.schema = schema
        self.use_sr = env_bool("SR_ENABLED", True)
        self._ctx = lambda topic: SerializationContext(topic, MessageField.VALUE)
        self.last_schema_id = None
        if not self.use_sr:
            return

        client = sr_client()
        conf = {
            "auto.register.schemas": env_bool("SR_AUTO_REGISTER", True),
            "use.latest.version": env_bool("SR_USE_LATEST", False),
        }
        subject = env("SR_SUBJECT")
        if subject:
            conf["subject.name.strategy"] = lambda ctx, record_name: subject
        if schema.fmt == "avro":
            from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

            self._ser = AvroSerializer(client, schema.text, conf=conf)
            self._de = AvroDeserializer(client)
        else:
            from confluent_kafka.schema_registry.json_schema import JSONSerializer

            self._ser = JSONSerializer(schema.text, client, conf=conf)
            self._de = None   # decoded by hand below, then validated against the local schema

    def encode(self, record, topic):
        self.schema.validate(record)
        if self.use_sr:
            return self._ser(record, self._ctx(topic))
        if self.schema.fmt == "avro":
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, self.schema.parsed, record)
            return buf.getvalue()
        return json.dumps(record, default=str).encode("utf-8")

    def decode(self, data, topic):
        if self.use_sr:
            if not data or data[0] != 0:
                raise ValueError("payload does not start with the Schema Registry magic byte")
            self.last_schema_id = int.from_bytes(data[1:5], "big")
            if self.schema.fmt == "avro":
                record = self._de(data, self._ctx(topic))
            else:
                record = json.loads(data[5:].decode("utf-8"))
        elif self.schema.fmt == "avro":
            record = fastavro.schemaless_reader(io.BytesIO(data), self.schema.parsed)
        else:
            record = json.loads(data.decode("utf-8"))
        if self.schema.fmt == "jsonschema":
            self.schema.validate(record)
        return record


# --------------------------------------------------------------------------
# topic handling
# --------------------------------------------------------------------------
def ensure_topic(topic):
    if not env_bool("TOPIC_CREATE", False):
        return
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(kafka_conf())
    new = NewTopic(
        topic,
        num_partitions=env_int("TOPIC_PARTITIONS", 3),
        replication_factor=env_int("TOPIC_REPLICATION_FACTOR", 3),
        config=json.loads(env("TOPIC_CONFIG", "{}")),
    )
    for name, future in admin.create_topics([new]).items():
        try:
            future.result(timeout=60)
            log.info("created topic %s", name)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("topic %s already exists", name)
            else:
                raise


# --------------------------------------------------------------------------
# produce
# --------------------------------------------------------------------------
class Stop:
    requested = False


def install_signal_handlers():
    def handler(signum, frame):
        log.info("signal %s received, stopping", signum)
        Stop.requested = True

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def make_key(mode, index, record, rnd):
    if mode == "none":
        return None
    if mode == "uuid":
        return str(uuid.UUID(int=rnd.getrandbits(128), version=4))
    if mode == "sequence":
        return str(index)
    if mode.startswith("fixed:"):
        return mode[len("fixed:"):]
    if mode.startswith("field:"):
        field = mode[len("field:"):]
        if not isinstance(record, dict) or field not in record:
            raise ConfigError(f"key mode {mode!r}: record has no top-level field {field!r}")
        return str(record[field])
    raise ConfigError(f"unknown key mode {mode!r} (use none, uuid, sequence, fixed:<v> or field:<name>)")


def produce_messages(codec, gen, topic, count, rate, key_mode, rnd, key_fn=None):
    """Send `count` records (0 = until stopped). Returns a stats dict."""
    from confluent_kafka import Producer

    conf = kafka_conf()
    conf.setdefault("acks", env("PRODUCER_ACKS", "all"))
    conf.setdefault("linger.ms", 5)
    conf.update(json.loads(env("PRODUCER_EXTRA_PROPS", "{}")))
    producer = Producer(conf)

    stats = {"sent": 0, "delivered": 0, "failed": 0, "bytes": 0}
    samples = env_int("LOG_SAMPLES", 3)

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            log.error("delivery failed: %s", err)
        else:
            stats["delivered"] += 1
            stats["bytes"] += len(msg.value() or b"")

    start = time.monotonic()
    index = 0
    while not Stop.requested and (count == 0 or index < count):
        record = gen.record()
        key = key_fn(index) if key_fn else make_key(key_mode, index, record, rnd)
        payload = codec.encode(record, topic)
        if index < samples:
            log.info("sample record %d key=%s: %s", index, key, short(as_json(record)))
        while True:
            try:
                producer.produce(topic, key=key, value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(1.0)
        producer.poll(0)
        index += 1
        stats["sent"] = index
        if rate > 0:
            delay = start + index / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        if index % 100 == 0:
            log.info("progress: sent=%d delivered=%d failed=%d", stats["sent"], stats["delivered"], stats["failed"])

    remaining = producer.flush(60)
    if remaining:
        log.error("%d message(s) were not delivered before the flush timeout", remaining)
        stats["failed"] += remaining
    stats["seconds"] = round(time.monotonic() - start, 2)
    return stats


# --------------------------------------------------------------------------
# consume
# --------------------------------------------------------------------------
def make_consumer(group, reset, on_assign=None):
    from confluent_kafka import Consumer

    conf = kafka_conf()
    conf.update({
        "group.id": group,
        "auto.offset.reset": reset,
        "enable.auto.commit": env_bool("CONSUMER_AUTO_COMMIT", True),
    })
    conf.update(json.loads(env("CONSUMER_EXTRA_PROPS", "{}")))
    return Consumer(conf)


def consume_messages(codec, topic, group, reset, max_messages, idle_timeout):
    from confluent_kafka import KafkaError

    consumer = make_consumer(group, reset)
    consumer.subscribe([topic])
    stats = {"received": 0, "valid": 0, "invalid": 0}
    samples = env_int("LOG_SAMPLES", 3)
    last_message = time.monotonic()
    try:
        while not Stop.requested:
            msg = consumer.poll(1.0)
            if msg is None:
                if idle_timeout and time.monotonic() - last_message > idle_timeout:
                    log.info("no messages for %ss, stopping", idle_timeout)
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            last_message = time.monotonic()
            stats["received"] += 1
            try:
                record = codec.decode(msg.value(), topic)
                stats["valid"] += 1
                if stats["valid"] <= samples:
                    log.info("record p%d@%d key=%s schema_id=%s: %s", msg.partition(), msg.offset(),
                             msg.key().decode("utf-8", "replace") if msg.key() else None,
                             codec.last_schema_id, short(as_json(record)))
            except Exception as exc:
                stats["invalid"] += 1
                log.error("p%d@%d does not decode/conform: %s", msg.partition(), msg.offset(), exc)
            if stats["received"] % 100 == 0:
                log.info("progress: %s", as_json(stats))
            if max_messages and stats["received"] >= max_messages:
                break
    finally:
        consumer.close()
    return stats


# --------------------------------------------------------------------------
# roundtrip (used by `helm test`)
# --------------------------------------------------------------------------
def roundtrip(codec, gen, topic, count, rnd):
    from confluent_kafka import KafkaError

    run_id = uuid.uuid4().hex[:8]
    group = f"ktc-roundtrip-{run_id}"
    assigned = {"done": False}

    def on_assign(consumer, partitions):
        # start at the current end so only this run's messages are read
        for part in partitions:
            _, high = consumer.get_watermark_offsets(part, timeout=15)
            part.offset = high
        consumer.assign(partitions)
        assigned["done"] = True

    consumer = make_consumer(group, "latest")
    consumer.subscribe([topic], on_assign=on_assign)
    deadline = time.monotonic() + env_int("ROUNDTRIP_ASSIGN_TIMEOUT", 90)
    while not assigned["done"]:
        consumer.poll(1.0)
        if time.monotonic() > deadline:
            consumer.close()
            raise RuntimeError("consumer was not assigned any partition in time")
    log.info("roundtrip %s: consumer ready, producing %d record(s)", run_id, count)

    prefix = f"rt-{run_id}-"
    expected = {f"{prefix}{i}" for i in range(count)}
    stats = produce_messages(codec, gen, topic, count, 0, "sequence", rnd, key_fn=lambda i: f"{prefix}{i}")
    if stats["failed"]:
        consumer.close()
        raise RuntimeError(f"producer reported {stats['failed']} failed delivery(ies)")

    seen, invalid = set(), 0
    deadline = time.monotonic() + env_int("ROUNDTRIP_TIMEOUT", 120)
    try:
        while seen != expected and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            key = msg.key().decode("utf-8", "replace") if msg.key() else None
            if key not in expected:
                continue
            try:
                codec.decode(msg.value(), topic)
                seen.add(key)
            except Exception as exc:
                invalid += 1
                log.error("roundtrip record %s does not conform: %s", key, exc)
    finally:
        consumer.close()

    result = {"run": run_id, "produced": count, "consumed_valid": len(seen), "invalid": invalid,
              "missing": len(expected - seen)}
    log.info("roundtrip result: %s", as_json(result))
    return result["missing"] == 0 and invalid == 0


# --------------------------------------------------------------------------
# modes
# --------------------------------------------------------------------------
def build_rnd():
    seed = env("SEED")
    rnd = random.Random(int(seed)) if seed not in (None, "") else random.Random()
    return rnd


def mode_sample():
    schema = Schema.load()
    gen = make_generator(schema, build_rnd())
    record = gen.record()
    schema.validate(record)
    print(as_json(record))
    return 0


def mode_check():
    schema = Schema.load()
    gen = make_generator(schema, build_rnd())
    schema.validate(gen.record())
    log.info("schema (%s) parses and generated data validates", schema.fmt)

    from confluent_kafka.admin import AdminClient

    admin = AdminClient(kafka_conf())
    topic = env("TOPIC", required=True)
    meta = admin.list_topics(timeout=20)
    log.info("connected to Kafka: %d broker(s), cluster id %s", len(meta.brokers), meta.cluster_id)
    if topic in meta.topics:
        log.info("topic %s exists with %d partition(s)", topic, len(meta.topics[topic].partitions))
    else:
        log.warning("topic %s does not exist (set topic.create=true or create it first)", topic)
    if env_bool("SR_ENABLED", True):
        subjects = sr_client().get_subjects()
        log.info("Schema Registry reachable, %d subject(s)", len(subjects))
    return 0


def mode_produce():
    schema = Schema.load()
    rnd = build_rnd()
    gen = make_generator(schema, rnd)
    codec = Codec(schema)
    topic = env("TOPIC", required=True)
    ensure_topic(topic)
    stats = produce_messages(
        codec, gen, topic,
        count=env_int("MESSAGE_COUNT", 100),
        rate=env_float("RATE_PER_SECOND", 10),
        key_mode=env("KEY_MODE", "uuid"),
        rnd=rnd,
    )
    log.info("producer finished: %s", as_json(stats))
    return 1 if stats["failed"] else 0


def mode_consume():
    schema = Schema.load()
    codec = Codec(schema)
    topic = env("TOPIC", required=True)
    stats = consume_messages(
        codec, topic,
        group=env("CONSUMER_GROUP", required=True),
        reset=env("AUTO_OFFSET_RESET", "earliest"),
        max_messages=env_int("MAX_MESSAGES", 0),
        idle_timeout=env_int("IDLE_TIMEOUT_SECONDS", 0),
    )
    log.info("consumer finished: %s", as_json(stats))
    return 1 if stats["invalid"] and env_bool("FAIL_ON_INVALID", True) else 0


def mode_roundtrip():
    schema = Schema.load()
    rnd = build_rnd()
    gen = make_generator(schema, rnd)
    codec = Codec(schema)
    topic = env("TOPIC", required=True)
    ensure_topic(topic)
    if env_bool("PREFLIGHT", True):
        mode_check()
    return 0 if roundtrip(codec, gen, topic, env_int("MESSAGE_COUNT", 10), rnd) else 1


MODES = {
    "sample": mode_sample,
    "check": mode_check,
    "produce": mode_produce,
    "consume": mode_consume,
    "roundtrip": mode_roundtrip,
}


def main(argv):
    logging.basicConfig(
        level=getattr(logging, env("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    if len(argv) != 2 or argv[1] not in MODES:
        print(f"usage: client.py {{{','.join(MODES)}}}", file=sys.stderr)
        return 2
    install_signal_handlers()
    try:
        return MODES[argv[1]]()
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        return 2
    except Exception:
        log.exception("%s failed", argv[1])
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
