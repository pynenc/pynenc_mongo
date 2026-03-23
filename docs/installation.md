# Installation & Quick Start

## Install

```bash
pip install pynenc-mongo
```

The plugin registers itself automatically via the `pynenc.plugins` entry point — no extra configuration needed.

## Quick Start

### PynencBuilder (Recommended)

```python
from pynenc import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_app")
    .mongo(url="mongodb://localhost:27017/pynenc")
    .process_runner()
    .build()
)

@app.task
def add(x: int, y: int) -> int:
    return x + y

result = add(1, 2).result  # 3
```

### Environment Variables

```bash
PYNENC_MONGO_URL=mongodb://localhost:27017/pynenc pynenc worker
```

### Docker Compose

```yaml
services:
  mongo:
    image: mongo:7
    ports: ["27017:27017"]

  worker:
    build: .
    environment:
      PYNENC_MONGO_URL: mongodb://mongo:27017/pynenc
    depends_on: [mongo]
    command: pynenc worker
```

## Builder Methods

### `.mongo(url, db, host, port, username, password, auth_source)`

Configure all MongoDB components at once — orchestrator, broker, state backend, client data store, and trigger.

```python
# Using URL (recommended)
builder.mongo(url="mongodb://localhost:27017/pynenc")

# Using individual parameters
builder.mongo(host="localhost", port=27017, db="pynenc")

# With authentication
builder.mongo(host="localhost", port=27017, db="pynenc",
              username="user", password="pass")
```

| Parameter     | Type          | Description                                       |
| ------------- | ------------- | ------------------------------------------------- |
| `url`         | `str \| None` | Full MongoDB URL. Overrides all other parameters. |
| `db`          | `str \| None` | Database name. Ignored if `url` is set.           |
| `host`        | `str \| None` | Hostname. Ignored if `url` is set.                |
| `port`        | `int \| None` | Port. Ignored if `url` is set.                    |
| `username`    | `str \| None` | Auth username. Ignored if `url` is set.           |
| `password`    | `str \| None` | Auth password. Ignored if `url` is set.           |
| `auth_source` | `str \| None` | Auth source database. Ignored if `url` is set.    |

### `.mongo_client_data_store(min_size_to_cache, local_cache_size, max_size_to_cache)`

Override argument cache settings. Requires `.mongo()` to be called first.

```python
builder.mongo(url="mongodb://localhost:27017/pynenc").mongo_client_data_store(
    min_size_to_cache=2048,
    max_size_to_cache=8 * 1024 * 1024,
)
```

| Parameter           | Type  | Default    | Description                                        |
| ------------------- | ----- | ---------- | -------------------------------------------------- |
| `min_size_to_cache` | `int` | `1024`     | Minimum serialized size (chars) to trigger caching |
| `local_cache_size`  | `int` | `1024`     | Maximum items kept in the local in-process cache   |
| `max_size_to_cache` | `int` | `16777216` | Maximum cacheable argument size in bytes (16 MB)   |

### `.mongo_trigger(scheduler_interval_seconds, enable_scheduler)`

Fine-tune the trigger scheduler. Requires `.mongo()` to be called first.

```python
builder.mongo(url="mongodb://localhost:27017/pynenc").mongo_trigger(
    scheduler_interval_seconds=30,
)
```

| Parameter                    | Type   | Default | Description                                            |
| ---------------------------- | ------ | ------- | ------------------------------------------------------ |
| `scheduler_interval_seconds` | `int`  | `60`    | How often the scheduler checks for time-based triggers |
| `enable_scheduler`           | `bool` | `True`  | Whether to run the trigger scheduler at all            |
