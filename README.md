<p align="center">
  <img src="https://pynenc.org/assets/img/pynenc_logo.png" alt="Pynenc" width="300">
</p>
<h1 align="center">Pynenc MongoDB Plugin</h1>
<p align="center">
    <em>Full-stack MongoDB backend for Pynenc distributed task orchestration</em>
</p>
<p align="center">
    <a href="https://pypi.org/project/pynenc-mongo" target="_blank">
        <img src="https://img.shields.io/pypi/v/pynenc-mongo?color=%2334D058&label=pypi%20package" alt="Package version">
    </a>
    <a href="https://pypi.org/project/pynenc-mongo" target="_blank">
        <img src="https://img.shields.io/pypi/pyversions/pynenc-mongo.svg?color=%2334D058" alt="Supported Python versions">
    </a>
    <a href="https://github.com/pynenc/pynenc-mongodb/commits/main">
        <img src="https://img.shields.io/github/last-commit/pynenc/pynenc-mongodb" alt="GitHub last commit">
    </a>
    <a href="https://github.com/pynenc/pynenc-mongodb/blob/main/LICENSE">
        <img src="https://img.shields.io/github/license/pynenc/pynenc-mongodb" alt="GitHub license">
    </a>
</p>

---

**Documentation**: <a href="https://pynenc-mongodb.readthedocs.io" target="_blank">https://pynenc-mongodb.readthedocs.io</a>

**Pynenc Documentation**: <a href="https://docs.pynenc.org" target="_blank">https://docs.pynenc.org</a>

**Source Code**: <a href="https://github.com/pynenc/pynenc-mongodb" target="_blank">https://github.com/pynenc/pynenc-mongodb</a>

---

The `pynenc-mongo` plugin provides all five Pynenc backend components running on MongoDB, with automatic document chunking for large payloads and a pseudo-atomic ownership protocol for distributed invocation management.

## Components

| Component             | Class                  | Role                                                               |
| --------------------- | ---------------------- | ------------------------------------------------------------------ |
| **Orchestrator**      | `MongoOrchestrator`    | Invocation lifecycle, ownership consensus & blocking control       |
| **Broker**            | `MongoBroker`          | FIFO message queue using MongoDB collections                       |
| **State Backend**     | `MongoStateBackend`    | Persistent state, results & exceptions with auto document chunking |
| **Client Data Store** | `MongoClientDataStore` | Argument caching with compression for large payloads               |
| **Trigger**           | `MongoTrigger`         | Event-driven & cron-based scheduling with distributed claims       |

## Installation

```bash
pip install pynenc-mongo
```

The plugin registers itself automatically via Python entry points when installed.

## Quick Start

```python
from pynenc import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_app")
    .mongo(url="mongodb://localhost:27017/pynenc")  # all components on MongoDB
    .process_runner()
    .build()
)

@app.task
def add(x: int, y: int) -> int:
    return x + y

result = add(1, 2).result  # 3
```

`.mongo()` registers every component at once. Start a runner with:

```bash
pynenc --app=tasks.app runner start
```

## Configuration

### Builder Parameters

```python
# URL-based (recommended)
app = (
    PynencBuilder()
    .app_id("my_app")
    .mongo(url="mongodb://localhost:27017/pynenc")
    .multi_thread_runner(min_threads=2, max_threads=8)
    .build()
)

# Individual parameters
app = (
    PynencBuilder()
    .app_id("my_app")
    .mongo(
        host="localhost",
        port=27017,
        db="pynenc",
        username="admin",
        password="secret",
        auth_source="admin",
    )
    .process_runner()
    .build()
)
```

### Component-Specific Configuration

```python
app = (
    PynencBuilder()
    .app_id("my_app")
    .mongo(url="mongodb://localhost:27017/pynenc")
    .mongo_client_data_store(
        min_size_to_cache=1024,          # cache arguments > 1KB
        local_cache_size=1000,           # local LRU cache entries
        max_size_to_cache=16777216,      # max 16MB per document
    )
    .mongo_trigger(
        scheduler_interval_seconds=60,
        enable_scheduler=True,
    )
    .build()
)
```

### Environment Variables

```bash
PYNENC__MONGO__MONGO_URL="mongodb://localhost:27017/pynenc"
# Or individual parameters:
PYNENC__MONGO__MONGO_HOST="localhost"
PYNENC__MONGO__MONGO_PORT=27017
PYNENC__MONGO__MONGO_DB="pynenc"
```

### Connection URLs

```python
.mongo(url="mongodb://localhost:27017/pynenc")                          # Standard
.mongo(url="mongodb://user:pass@localhost:27017/pynenc?authSource=admin")  # With auth
.mongo(url="mongodb+srv://cluster.example.net/pynenc")                  # Atlas SRV
```

## Requirements

- Python >= 3.11
- Pynenc >= 0.1.0
- pymongo >= 3.12.2
- A running MongoDB server

## Related Plugins

- **[pynenc-redis](https://github.com/pynenc/pynenc-redis)**: Full-stack Redis backend
- **[pynenc-rabbitmq](https://github.com/pynenc/pynenc-rabbitmq)**: RabbitMQ broker (pairs with MongoDB for state/orchestrator/triggers)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
