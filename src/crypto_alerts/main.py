# src/crypto_alerts/main.py
#
# Application entry point. This file is intentionally thin -- it does
# nothing except configure the runtime environment and hand off to the
# pipeline coordinator. All business logic lives in the components.
#
# Startup sequence:
#   1. Apply Windows event loop policy if running on Windows
#   2. Configure structured logging
#   3. Load and validate settings from .env
#   4. Build the rule repository with seed rules
#   5. Build the feed, dispatchers, and coordinator
#   6. Register SIGINT/SIGTERM handlers for clean shutdown
#   7. Run the pipeline until interrupted

import asyncio
import logging
import signal
import sys
from decimal import Decimal

from crypto_alerts.config.settings import get_settings
from crypto_alerts.dispatcher.console_dispatcher import ConsoleDispatcher
from crypto_alerts.engine.rule_repository import RuleRepository
from crypto_alerts.feed.binance_feed import BinanceFeed
from crypto_alerts.models.alert_event import AlertEvent
from crypto_alerts.models.alert_rule import AlertRule, ConditionOperator
from crypto_alerts.models.price_tick import PriceTick
from crypto_alerts.pipeline.coordinator import PipelineCoordinator


def configure_logging(log_level: str) -> None:
    """
    Configures the root logger with a consistent format.

    Every log line includes the timestamp, level, logger name, and
    message. The logger name is the module path (e.g.
    crypto_alerts.engine.evaluator), which makes it trivial to trace
    which component produced a given log line.
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def apply_windows_event_loop_policy() -> None:
    """
    Switches asyncio to SelectorEventLoop on Windows.

    The default ProactorEventLoop on Windows uses I/O Completion Ports
    which are incompatible with the socket operations used by the
    websockets library. SelectorEventLoop uses the select() syscall
    which works correctly across all platforms.

    This must be called before any event loop is created -- i.e. before
    asyncio.run() -- otherwise it has no effect.
    """
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logging.getLogger(__name__).info(
            "Applied WindowsSelectorEventLoopPolicy"
        )


def build_seed_rules() -> list[AlertRule]:
    """
    Returns the initial set of alert rules loaded at startup.

    In a production system these would be loaded from a database or
    a configuration file. For this implementation they are defined here
    explicitly so the system runs out of the box with visible output.

    Every rule uses threshold Decimal('1.00') with operator ABOVE so
    any real price tick will fire it -- this makes it easy to confirm
    the full pipeline is working end to end.
    """
    return [
        AlertRule(
            symbol="BTCUSDT",
            operator=ConditionOperator.ABOVE,
            threshold=Decimal("1.00"),
            label="BTC live feed test",
        ),
        AlertRule(
            symbol="ETHUSDT",
            operator=ConditionOperator.ABOVE,
            threshold=Decimal("1.00"),
            label="ETH live feed test",
        ),
        AlertRule(
            symbol="BNBUSDT",
            operator=ConditionOperator.ABOVE,
            threshold=Decimal("1.00"),
            label="BNB live feed test",
        ),
    ]


async def main() -> None:
    """
    Async entry point. Builds and runs the pipeline until cancelled.

    Signal handling:
        On SIGINT (Ctrl+C) or SIGTERM, we cancel the pipeline task and
        allow the coordinator's shutdown sequence to drain the queues
        and close resources gracefully.

        On Windows, signal.add_signal_handler() is not available for
        all signals. We use a try/except to fall back to the default
        KeyboardInterrupt handling, which asyncio.run() already converts
        to a CancelledError on the main task.
    """
    settings = get_settings()
    logger = logging.getLogger(__name__)

    logger.info(
        "Crypto Alerts Engine starting -- env=%s log_level=%s",
        settings.app_env,
        settings.log_level,
    )

    # Build rule repository and load seed rules
    repository = RuleRepository()
    for rule in build_seed_rules():
        repository.add_rule(rule)

    logger.info(
        "Rule repository loaded -- total_rules=%d symbols=%s",
        repository.total_rule_count,
        repository.get_watched_symbols(),
    )

    # Build feed
    inbound_queue: asyncio.Queue[PriceTick] = asyncio.Queue(
        maxsize=settings.pipeline_queue_max_size
    )
    feed = BinanceFeed(outbound_queue=inbound_queue)
    await feed.subscribe(repository.get_watched_symbols())

    # Build dispatchers
    outbound_queue: asyncio.Queue[AlertEvent] = asyncio.Queue(
        maxsize=settings.pipeline_queue_max_size
    )
    dispatchers = [ConsoleDispatcher(inbound_queue=outbound_queue)]

    # Build coordinator
    coordinator = PipelineCoordinator(
        feed=feed,
        repository=repository,
        dispatchers=dispatchers,
    )

    # Create the pipeline task
    pipeline_task = asyncio.create_task(coordinator.run(), name="pipeline")

    # Register OS signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig_name: str) -> None:
        logger.info("Received signal %s -- initiating graceful shutdown", sig_name)
        pipeline_task.cancel()

    try:
        loop.add_signal_handler(
            signal.SIGINT, lambda: _request_shutdown("SIGINT")
        )
        loop.add_signal_handler(
            signal.SIGTERM, lambda: _request_shutdown("SIGTERM")
        )
    except NotImplementedError:
        # Windows does not support add_signal_handler for all signals.
        # asyncio.run() handles KeyboardInterrupt (Ctrl+C) natively on
        # Windows by cancelling the main task, so we do not need to
        # register it manually.
        logger.debug(
            "Signal handlers not supported on this platform -- "
            "using default KeyboardInterrupt handling"
        )

    # Run until cancelled or a fatal error occurs
    try:
        await pipeline_task
    except asyncio.CancelledError:
        logger.info("Pipeline task cancelled -- shutdown complete")
    except Exception as exc:
        logger.critical(
            "Pipeline exited with unhandled exception -- %s: %s",
            type(exc).__name__,
            exc,
        )
        sys.exit(1)


if __name__ == "__main__":
    apply_windows_event_loop_policy()
    configure_logging(get_settings().log_level)
    asyncio.run(main())