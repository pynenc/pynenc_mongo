:::{image} \_static/logo.png
:alt: Pynenc
:align: center
:height: 90px
:class: hero-logo
:::

# Pynenc MongoDB Plugin

**Full-stack MongoDB backend for [Pynenc](https://pynenc.readthedocs.io/) distributed task orchestration.**

The `pynenc-mongo` plugin provides all five Pynenc components running on MongoDB,
with automatic document chunking for large payloads and a pseudo-atomic ownership
protocol for distributed invocation management.

```bash
pip install pynenc-mongo
```

## Components at a Glance

| Component             | Class                  | Role                                                               |
| --------------------- | ---------------------- | ------------------------------------------------------------------ |
| **Orchestrator**      | `MongoOrchestrator`    | Invocation lifecycle, ownership consensus & blocking control       |
| **Broker**            | `MongoBroker`          | FIFO message queue using MongoDB collections                       |
| **State Backend**     | `MongoStateBackend`    | Persistent state, results & exceptions with auto document chunking |
| **Client Data Store** | `MongoClientDataStore` | Argument caching with compression for large payloads               |
| **Trigger**           | `MongoTrigger`         | Event-driven & cron-based scheduling with distributed claims       |

---

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
```

`.mongo()` registers every component at once. See {doc}`installation` for
environment-variable and Docker Compose alternatives.

---

::::{grid} 1 2 3 3
:gutter: 3
:padding: 0

:::{grid-item-card} 🚀 Installation & Quick Start
:link: installation
:link-type: doc
:shadow: sm

Get up and running with `PynencBuilder`, environment variables, and Docker Compose examples.
:::

:::{grid-item-card} ⚙️ Configuration Reference
:link: configuration
:link-type: doc
:shadow: sm

All connection, pool, retry, chunking, and orchestrator settings — with types, defaults, and descriptions.
:::

:::{grid-item-card} 🏗️ Architecture
:link: architecture
:link-type: doc
:shadow: sm

Document chunking, retryable operations, pseudo-atomic ownership, and the full collection layout.
:::
::::

---

Part of the **[Pynenc](https://pynenc.readthedocs.io/) ecosystem** ·
[Redis Plugin](https://pynenc-redis.readthedocs.io/) ·
[RabbitMQ Plugin](https://pynenc-rabbitmq.readthedocs.io/)

```{toctree}
:hidden:
:maxdepth: 2
:caption: MongoDB Plugin

installation
configuration
architecture
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: API Reference

apidocs/index.rst
```

```{toctree}
:hidden:
:caption: Pynenc Ecosystem

Pynenc Docs <https://pynenc.readthedocs.io/>
Redis Plugin <https://pynenc-redis.readthedocs.io/>
RabbitMQ Plugin <https://pynenc-rabbitmq.readthedocs.io/>
```
