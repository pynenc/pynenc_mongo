# Configuration Reference

All settings can be provided via the builder, environment variables, or YAML config files.
See the [Pynenc configuration guide](https://pynenc.readthedocs.io/en/latest/configuration/index.html) for the general mechanism.

## Connection Settings — `ConfigMongo`

| Setting             | Type  | Default       | Description                                                                                       |
| ------------------- | ----- | ------------- | ------------------------------------------------------------------------------------------------- |
| `mongo_url`         | `str` | `""`          | Full MongoDB URL, e.g. `mongodb://localhost:27017/pynenc`. Overrides all other connection fields. |
| `mongo_host`        | `str` | `"localhost"` | MongoDB server hostname                                                                           |
| `mongo_port`        | `int` | `27017`       | MongoDB server port                                                                               |
| `mongo_db`          | `str` | `"pynenc"`    | Database name                                                                                     |
| `mongo_username`    | `str` | `""`          | Authentication username                                                                           |
| `mongo_password`    | `str` | `""`          | Authentication password                                                                           |
| `mongo_auth_source` | `str` | `""`          | Authentication source database                                                                    |

## Connection Pool Settings

| Setting                      | Type  | Default | Description                                |
| ---------------------------- | ----- | ------- | ------------------------------------------ |
| `mongo_pool_max_connections` | `int` | `100`   | Maximum connections in the pool            |
| `socket_timeout`             | `int` | `10`    | Socket timeout for operations (seconds)    |
| `socket_connect_timeout`     | `int` | `10`    | Connection establishment timeout (seconds) |

## Retry Settings

| Setting              | Type    | Default | Description                                                |
| -------------------- | ------- | ------- | ---------------------------------------------------------- |
| `max_retries`        | `int`   | `3`     | Maximum retry attempts for transient failures              |
| `retry_base_delay`   | `float` | `0.1`   | Initial delay between retries (seconds)                    |
| `retry_max_delay`    | `float` | `60.0`  | Maximum delay cap between retries (seconds)                |
| `retry_max_time`     | `float` | `300.0` | Total maximum time spent retrying (seconds)                |
| `retry_indefinitely` | `bool`  | `False` | Retry forever — ignores `max_retries` and `retry_max_time` |

## Data Chunking Settings

| Setting           | Type  | Default    | Description                                                                |
| ----------------- | ----- | ---------- | -------------------------------------------------------------------------- |
| `chunk_threshold` | `int` | `15728640` | Document size threshold (15 MB) above which data is compressed and chunked |

## Orchestrator Settings — `ConfigOrchestratorMongo`

| Setting                             | Type    | Default | Description                                                  |
| ----------------------------------- | ------- | ------- | ------------------------------------------------------------ |
| `auto_final_invocation_purge_hours` | `int`   | `24`    | Auto-purge completed invocations after this many hours       |
| `ownership_consensus_wait_seconds`  | `float` | `0.1`   | Wait time for the pseudo-atomic ownership consensus protocol |

## Environment Variables

Every setting maps to `PYNENC_{SETTING_UPPERCASE}`:

```bash
export PYNENC_MONGO_URL="mongodb://localhost:27017/pynenc"
export PYNENC_MONGO_PASSWORD="mysecret"
export PYNENC_MONGO_POOL_MAX_CONNECTIONS=50
export PYNENC_MAX_RETRIES=5
```

## YAML Configuration

```yaml
# pynenc.yaml
mongo_url: "mongodb://localhost:27017/pynenc"
mongo_pool_max_connections: 50
max_retries: 5
ownership_consensus_wait_seconds: 0.05
```
