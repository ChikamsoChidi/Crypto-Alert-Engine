# src/crypto_alerts/engine/rule_repository.py
#
# RuleRepository is the single source of truth for all active alert rules.
# It is designed to be safe for concurrent reads from multiple coroutines.
#
# We store rules in a dict keyed by symbol, where each value is itself a
# dict keyed by rule_id. This two-level structure means looking up all
# rules for a given symbol is O(1), no scanning the entire rule set on
# every price tick.
#
# Structure:
#   {
#     "BTCUSDT": {
#       "rule-uuid-1": AlertRule(...),
#       "rule-uuid-2": AlertRule(...),
#     },
#     "ETHUSDT": {
#       "rule-uuid-3": AlertRule(...),
#     }
#   }

import logging
from collections import defaultdict

from crypto_alerts.models.alert_rule import AlertRule

logger = logging.getLogger(__name__)


class RuleRepository:
    """
    An in-memory store for active AlertRule instances.

    All mutating methods (add, remove) are synchronous because dict
    operations in CPython are protected by the GIL, making them
    effectively atomic for single operations. We do not need asyncio
    locks here -- but if this were ever backed by a database or Redis,
    the methods would become async and acquire a lock before writing.
    """

    def __init__(self) -> None:
        # defaultdict means we never have to check if a symbol key
        # exists before inserting the first rule for that symbol
        self._rules: defaultdict[str, dict[str, AlertRule]] = defaultdict(dict)

    def add_rule(self, rule: AlertRule) -> None:
        """
        Adds a rule to the repository.
        If a rule with the same rule_id already exists, it is replaced.
        """
        self._rules[rule.symbol][rule.rule_id] = rule
        logger.info(
            "Rule added -- id=%s symbol=%s operator=%s threshold=%s",
            rule.rule_id,
            rule.symbol,
            rule.operator,
            rule.threshold,
        )

    def remove_rule(self, rule_id: str, symbol: str) -> bool:
        """
        Removes a rule by its id and symbol.

        Returns True if the rule was found and removed, False if it did
        not exist. The caller can use the return value to detect
        double-remove bugs without raising an exception.
        """
        symbol_rules = self._rules.get(symbol)
        if symbol_rules is None or rule_id not in symbol_rules:
            logger.warning(
                "Attempted to remove non-existent rule -- id=%s symbol=%s",
                rule_id,
                symbol,
            )
            return False

        del symbol_rules[rule_id]

        # Clean up the symbol key if no rules remain for it.
        # This prevents the dict from growing unboundedly over time
        # as symbols are added and removed.
        if not symbol_rules:
            del self._rules[symbol]

        logger.info("Rule removed -- id=%s symbol=%s", rule_id, symbol)
        return True

    def get_rules_for_symbol(self, symbol: str) -> list[AlertRule]:
        """
        Returns all active rules for a given symbol.

        Returns an empty list if no rules exist for the symbol, so the
        caller never has to handle a None return value.

        We return a list copy rather than a dict view so the caller
        cannot accidentally mutate the internal store.
        """
        return list(self._rules.get(symbol, {}).values())

    def get_all_rules(self) -> list[AlertRule]:
        """
        Returns every active rule across all symbols.
        Primarily used for diagnostics and admin endpoints.
        """
        return [
            rule
            for symbol_rules in self._rules.values()
            for rule in symbol_rules.values()
        ]

    def get_watched_symbols(self) -> set[str]:
        """
        Returns the set of symbols that currently have at least one rule.
        The feed layer uses this to know which WebSocket streams to subscribe to.
        """
        return set(self._rules.keys())

    @property
    def total_rule_count(self) -> int:
        """Returns the total number of active rules across all symbols."""
        return sum(len(rules) for rules in self._rules.values())