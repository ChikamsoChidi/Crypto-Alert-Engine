# Crypto Alerts Engine

A production-grade, real-time cryptocurrency price alerting system built in Python. The engine connects to the Binance WebSocket API, evaluates live price ticks against user-defined alert rules, and dispatches notifications through configurable delivery channels.

This project was built as a deliberate employability exercise demonstrating enterprise-grade backend engineering standards: strict static typing via Mypy, runtime data validation via Pydantic v2, high-concurrency asynchrony via asyncio, clean architecture with clearly separated concerns, and robust error handling throughout.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Component Breakdown](#component-breakdown)
- [Data Flow](#data-flow)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Engine](#running-the-engine)
- [Running the Tests](#running-the-tests)
- [Design Decisions](#design-decisions)
- [Extending the System](#extending-the-system)
- [Technical Trade-offs](#technical-trade-offs)

---

## Architecture Overview

The engine is structured as a three-stage asynchronous pipeline:

```
[Binance WebSocket] --> [BinanceFeed] --> [inbound_queue] --> [Evaluator] --> [outbound_queue] --> [Dispatchers]
                                                                    |
                                                           [RuleRepository]
```

Each stage is fully decoupled. The feed layer knows nothing about rules. The evaluator knows nothing about delivery channels. The dispatchers know nothing about where events came from. All three stages communicate exclusively through bounded `asyncio.Queue` instances, which act as the contract between them.

This decoupling is the central architectural decision of the system. It means any stage can be replaced, tested in isolation, or scaled independently without touching the others.

---

## Project Structure

```
crypto_alerts/
|
|-- src/
|   |-- crypto_alerts/
|   |   |-- __init__.py
|   |   |-- main.py                      # Application entry point
|   |   |
|   |   |-- config/
|   |   |   |-- __init__.py
|   |   |   |-- settings.py              # Pydantic BaseSettings configuration model
|   |   |
|   |   |-- feed/
|   |   |   |-- __init__.py
|   |   |   |-- base.py                  # Abstract base class for all price feeds
|   |   |   |-- binance_feed.py          # Binance WebSocket implementation
|   |   |
|   |   |-- models/
|   |   |   |-- __init__.py
|   |   |   |-- price_tick.py            # Canonical price tick domain model
|   |   |   |-- alert_rule.py            # Alert rule domain model
|   |   |   |-- alert_event.py           # Fired alert event model
|   |   |
|   |   |-- engine/
|   |   |   |-- __init__.py
|   |   |   |-- evaluator.py             # Core rule evaluation pipeline stage
|   |   |   |-- rule_repository.py       # In-memory rule store
|   |   |
|   |   |-- dispatcher/
|   |   |   |-- __init__.py
|   |   |   |-- base.py                  # Abstract dispatcher interface
|   |   |   |-- console_dispatcher.py    # Stdout delivery via logging
|   |   |   |-- webhook_dispatcher.py    # HTTP POST delivery with retry logic
|   |   |
|   |   |-- pipeline/
|   |       |-- __init__.py
|   |       |-- coordinator.py           # Composition root, lifecycle management
|
|-- tests/
|   |-- __init__.py
|   |-- unit/
|   |   |-- test_evaluator.py
|   |   |-- test_rule_repository.py
|   |   |-- test_models.py
|   |-- integration/
|       |-- test_pipeline.py
|
|-- .env
|-- .env.example
|-- requirements.txt
|-- requirements-dev.txt
|-- mypy.ini
|-- setup.py
|-- README.md
```

The strict rule governing imports is that only `pipeline/coordinator.py` is permitted to import from more than one package. Every other module imports only from its immediate dependencies. This discipline makes each package self-contained and independently understandable.

---

## Component Breakdown

### config/settings.py

Defines a `Settings` class backed by Pydantic `BaseSettings`. All configuration is read from environment variables and the `.env` file at startup, validated for type and range, and frozen into an immutable singleton via `lru_cache`. No other module reads from `os.environ` directly.

### models/

The three domain models that every component communicates through:

**`PriceTick`** represents a single price update received from the exchange. It uses `Decimal` for price and volume fields to guarantee exact arithmetic. The `timestamp` field normalizes both millisecond epoch integers and `datetime` objects into timezone-aware UTC datetimes. The model is frozen and immutable.

**`AlertRule`** represents a user-defined condition. Each rule specifies a symbol, a comparison operator (`ABOVE` or `BELOW`), a threshold price, and an optional human-readable label. The `evaluate(tick)` method encapsulates the comparison logic, so the engine never reimplements it.

**`AlertEvent`** is produced when a rule fires. It carries the full `AlertRule` and the `PriceTick` that triggered it, giving downstream dispatchers complete context for formatting notifications. It is immutable and safe to pass to multiple dispatchers concurrently.

### feed/

**`AbstractFeed`** defines the interface every feed must implement: a `run()` coroutine that produces ticks indefinitely and a `subscribe()` method that sets the symbols to watch.

**`BinanceFeed`** implements `AbstractFeed` against the Binance combined stream WebSocket endpoint. It builds the stream URL from subscribed symbols, connects, and normalizes each raw trade message into a `PriceTick`. It handles reconnection automatically with configurable delay and maximum attempt limits. Malformed or unrecognized messages are logged and skipped without crashing the feed.

### engine/

**`RuleRepository`** is a thread-safe in-memory store indexed by symbol. Rules are stored in a two-level dictionary: `{symbol: {rule_id: AlertRule}}`. This means looking up all rules for a given symbol is O(1) regardless of total rule count. It provides methods to add, remove, and query rules, and exposes the set of watched symbols so the feed knows which streams to subscribe to.

**`Evaluator`** is a long-running async worker that consumes `PriceTick` objects from the inbound queue and produces `AlertEvent` objects onto the outbound queue. For each tick, it retrieves only the rules for that tick's symbol and calls `rule.evaluate(tick)` on each one. It tracks processed and fired counts for runtime diagnostics.

### dispatcher/

**`AbstractDispatcher`** provides a base `run()` loop that handles queue consumption, error catching, logging, and `task_done()` bookkeeping. Subclasses only need to implement `dispatch(event)`.

**`ConsoleDispatcher`** writes alert events to stdout through the standard logging system. It is used in development to confirm the pipeline is producing events.

**`WebhookDispatcher`** delivers alert events as JSON POST requests to a configurable HTTP endpoint. It maintains a single `aiohttp.ClientSession` across all requests for connection pool efficiency. On transient server errors (5xx) or network failures, it retries with exponential backoff up to a configurable maximum. On permanent client errors (4xx), it logs and gives up immediately.

### pipeline/coordinator.py

The composition root. It is the only file that imports from multiple packages. It creates the two bounded queues, wires them to the feed and dispatchers, creates an `asyncio.Task` for each component, and runs them concurrently via `asyncio.gather`.

On shutdown, it follows a strict staged sequence:

1. Cancel the feed task to stop new ticks from entering the pipeline.
2. Drain the inbound queue so the evaluator processes every tick already received.
3. Drain the outbound queue so dispatchers deliver every event already fired.
4. Cancel the evaluator and dispatcher tasks.
5. Close any open external resources such as aiohttp sessions.

This sequence guarantees no in-flight data is lost on graceful shutdown.

### main.py

The entry point. It applies the Windows event loop policy, configures structured logging, loads settings, builds and wires all components, registers OS signal handlers for graceful shutdown, and starts the pipeline. It is intentionally thin -- all business logic lives in the components it assembles.

---

## Data Flow

A single price update travels through the system as follows:

1. Binance sends a raw JSON trade message over the WebSocket connection.
2. `BinanceFeed._parse_message()` validates and normalizes it into a `PriceTick`.
3. The tick is placed onto the bounded `inbound_queue` via `await queue.put()`. If the queue is full, this call blocks, applying back-pressure to the feed.
4. `Evaluator._process_next_tick()` pulls the tick from the inbound queue.
5. It retrieves all `AlertRule` instances for the tick's symbol from `RuleRepository`.
6. For each matching rule, it calls `rule.evaluate(tick)`. If the condition is satisfied, it creates an `AlertEvent` and places it onto the `outbound_queue`.
7. Each `AbstractDispatcher.run()` loop pulls the event from the outbound queue and calls `dispatch(event)`.
8. `ConsoleDispatcher` logs the event summary. `WebhookDispatcher` POSTs it to the configured URL.

---

## Prerequisites

- Python 3.11 or higher
- An active internet connection for the Binance WebSocket feed
- Windows, macOS, or Linux

---

## Installation

Clone the repository and create a virtual environment:

```bash
git clone <your-repo-url>
cd crypto_alerts

python -m venv .venv
```

Activate the virtual environment:

```bash
# Windows PowerShell
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

If you are on Windows and see an execution policy error:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Install dependencies and register the package:

```bash
pip install --upgrade pip
pip install -r requirements.txt -r requirements-dev.txt
pip install -e .
```

The `pip install -e .` step registers `src/` as the package root so Python can find `crypto_alerts` from anywhere without manually setting `PYTHONPATH`.

---

## Configuration

Copy the example environment file and edit it:

```bash
# Windows
copy .env.example .env

# macOS / Linux
cp .env.example .env
```

Available settings:

| Variable | Default | Description |
|---|---|---|
| `APP_ENV` | `development` | Runtime environment: development, staging, or production |
| `LOG_LEVEL` | `INFO` | Logging verbosity: DEBUG, INFO, WARNING, ERROR, or CRITICAL |
| `BINANCE_WS_BASE_URL` | `wss://stream.binance.com:9443` | Binance WebSocket base URL |
| `FEED_RECONNECT_DELAY_SECONDS` | `5.0` | Seconds to wait between reconnect attempts |
| `FEED_MAX_RECONNECT_ATTEMPTS` | `10` | Maximum consecutive reconnect failures before stopping |
| `PIPELINE_QUEUE_MAX_SIZE` | `1000` | Maximum capacity of each internal asyncio queue |

All values are validated by Pydantic at startup. An invalid value (for example, a non-numeric `FEED_RECONNECT_DELAY_SECONDS`) will raise a descriptive error before the pipeline starts.

---

## Running the Engine

```bash
python src/crypto_alerts/main.py
```

On startup you will see structured log output confirming the event loop policy, loaded rules, and WebSocket connection. Once connected, alert events are logged to stdout as they fire.

To stop the engine cleanly, press `Ctrl+C`. The coordinator will drain both queues and close all resources before the process exits.

Sample output:

```
2026-01-01 12:00:00 | INFO     | crypto_alerts.main | Applied WindowsSelectorEventLoopPolicy
2026-01-01 12:00:00 | INFO     | crypto_alerts.main | Crypto Alerts Engine starting -- env=development log_level=INFO
2026-01-01 12:00:00 | INFO     | crypto_alerts.main | Rule repository loaded -- total_rules=3 symbols={'BTCUSDT', 'ETHUSDT', 'BNBUSDT'}
2026-01-01 12:00:01 | INFO     | BinanceFeed | Connected to Binance WebSocket successfully
2026-01-01 12:00:01 | WARNING  | ConsoleDispatcher | [ALERT] (BTC live feed test) BTCUSDT is ABOVE threshold 1.00 -- current price: 67423.51 at 2026-01-01 12:00:01 UTC
2026-01-01 12:00:01 | WARNING  | ConsoleDispatcher | [ALERT] (ETH live feed test) ETHUSDT is ABOVE threshold 1.00 -- current price: 3512.88 at 2026-01-01 12:00:01 UTC
```

---

## Running the Tests

```bash
pytest tests/
```

For verbose output:

```bash
pytest tests/ -v
```

For type checking and linting:

```bash
mypy src/
ruff check src/
```

All three should complete with zero errors or warnings on a clean build.

---

## Design Decisions

### Bounded queues for back-pressure

Both internal queues have a fixed maximum size controlled by `PIPELINE_QUEUE_MAX_SIZE`. When a queue is full, `await queue.put()` blocks the producer naturally. This means if the evaluator falls behind the feed, the feed slows down rather than consuming unbounded memory. If the dispatcher falls behind the evaluator, the evaluator slows down. Back-pressure propagates upstream through the pipeline automatically without any explicit coordination.

### Decimal over float for financial data

All price and volume fields use Python's `Decimal` type rather than `float`. IEEE 754 floating-point arithmetic cannot represent most decimal fractions exactly, which causes rounding errors to accumulate silently over repeated comparisons. `Decimal` provides exact representation and predictable comparison behavior, which is a correctness requirement for any financial application.

### Frozen Pydantic models

All three domain models are configured with `frozen=True`. This makes instances immutable and hashable after construction. The practical effect in an async system is that a `PriceTick` or `AlertEvent` can be passed to multiple concurrent coroutines with a guarantee that no coroutine can mutate the data another is reading. It eliminates an entire category of concurrency bugs.

### Abstract base classes for the feed and dispatcher

Neither the coordinator nor any other component imports `BinanceFeed` or `ConsoleDispatcher` directly except where they are constructed in `main.py`. All other references are typed as `AbstractFeed` and `AbstractDispatcher`. This means the Binance feed can be swapped for a mock feed in tests, or a Coinbase feed in production, by changing only the construction site in `main.py`.

### Two-level rule indexing by symbol

`RuleRepository` stores rules as `{symbol: {rule_id: AlertRule}}` rather than a flat list. This means retrieving all rules for a given symbol is O(1) regardless of how many total rules exist. In a system processing hundreds of ticks per second across dozens of symbols, a flat scan would become measurable overhead. The two-level index eliminates it entirely.

### Re-raising CancelledError

Every `except asyncio.CancelledError` block in the codebase re-raises after logging. This is a hard requirement of the asyncio runtime. `CancelledError` is how asyncio signals a task to stop. If a coroutine catches it and does not re-raise, the task appears to keep running from asyncio's perspective even though it has stopped, causing `await task` to hang indefinitely. Swallowing `CancelledError` is one of the most common and hardest-to-debug errors in async Python.

### Staged shutdown sequence

The coordinator shuts down components in a specific order rather than cancelling everything simultaneously. Cancelling all tasks at once would lose any ticks sitting in the inbound queue and any events sitting in the outbound queue at the moment of shutdown. The staged sequence -- stop feed, drain inbound, drain outbound, cancel evaluator and dispatchers -- guarantees that every piece of data that entered the pipeline is fully processed before the process exits.

---

## Extending the System

### Adding a new alert rule at runtime

Obtain a reference to the `RuleRepository` and call `add_rule()`:

```python
from decimal import Decimal
from crypto_alerts.models.alert_rule import AlertRule, ConditionOperator

rule = AlertRule(
    symbol="SOLUSDT",
    operator=ConditionOperator.BELOW,
    threshold=Decimal("100.00"),
    label="SOL support level"
)
repository.add_rule(rule)
```

### Adding a new dispatcher

Create a class that extends `AbstractDispatcher` and implements `dispatch()`:

```python
from crypto_alerts.dispatcher.base import AbstractDispatcher
from crypto_alerts.models.alert_event import AlertEvent

class SlackDispatcher(AbstractDispatcher):
    async def dispatch(self, event: AlertEvent) -> None:
        # post to Slack API
        ...
```

Pass an instance to `PipelineCoordinator` in `main.py` alongside the existing dispatchers. No other changes are required.

### Replacing the feed

Create a class that extends `AbstractFeed` and implements `run()` and `subscribe()`. Pass it to `PipelineCoordinator` in place of `BinanceFeed`. The evaluator, repository, and dispatchers are completely unaffected.

### Loading rules from a database

Replace the `build_seed_rules()` function in `main.py` with an async function that queries a database and constructs `AlertRule` instances from the results. The rest of the startup sequence is unchanged.

---

## Technical Trade-offs

### Single shared outbound queue vs fan-out

The current design uses a single `outbound_queue` shared by all dispatchers. This means each `AlertEvent` is delivered to exactly one dispatcher in round-robin fashion. If the requirement is that every dispatcher receives every event, the coordinator would need to maintain one queue per dispatcher and have the evaluator push to all of them. The current design is simpler and sufficient for the use cases implemented. The fan-out design would add a small but real amount of memory overhead per additional dispatcher.

### In-memory rule store vs persistent store

`RuleRepository` stores rules in a Python dictionary. Rules do not survive a process restart. For a production system, rules would be persisted to a database such as PostgreSQL and loaded at startup, with an API layer allowing rules to be created, updated, and deleted at runtime. The repository interface is already abstracted behind method boundaries, so swapping the backing store requires only changes inside `rule_repository.py`.

### Single-process vs distributed

The entire pipeline runs in a single Python process. For most alerting workloads this is entirely sufficient. If throughput requirements exceeded single-process capacity, the natural evolution would be to replace the in-process `asyncio.Queue` instances with a message broker such as Redis Streams or Apache Kafka, run the feed, evaluator, and dispatchers as separate processes or services, and use the same domain models as the message schema. The component boundaries are already drawn at the right seams for this transition.
