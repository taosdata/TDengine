"""Grammar coverage tracking for the random SQL generator."""

from typing import Dict, List, Tuple


class CoverageTracker:
    """Track which grammar rule alternatives have been exercised."""

    def __init__(self, grammar: dict):
        """Initialize from grammar dict: {rule_name: [alt0, alt1, ...]}"""
        self._counts: Dict[Tuple[str, int], int] = {}
        for rule, alts in grammar.items():
            for i in range(len(alts)):
                self._counts[(rule, i)] = 0
        self._total_generated = 0
        self._errors_known = 0
        self._errors_unknown = 0

    def record(self, rule: str, alt_idx: int):
        """Record that a specific rule alternative was chosen."""
        key = (rule, alt_idx)
        if key in self._counts:
            self._counts[key] += 1

    def record_query(self):
        """Record that a query was successfully generated."""
        self._total_generated += 1

    def record_error(self, known: bool):
        """Record an error during query execution."""
        if known:
            self._errors_known += 1
        else:
            self._errors_unknown += 1

    def get_count(self, rule: str, alt_idx: int) -> int:
        """Get the number of times a specific alternative was chosen."""
        return self._counts.get((rule, alt_idx), 0)

    def uncovered(self) -> List[Tuple[str, int]]:
        """Return list of (rule, alt_idx) never triggered."""
        return [(r, i) for (r, i), c in self._counts.items() if c == 0]

    def coverage_rate(self) -> float:
        """Return coverage percentage (0.0 to 100.0)."""
        total = len(self._counts)
        if total == 0:
            return 100.0
        covered = sum(1 for c in self._counts.values() if c > 0)
        return covered / total * 100

    def report(self) -> str:
        """Generate a human-readable coverage report."""
        total = len(self._counts)
        covered = sum(1 for c in self._counts.values() if c > 0)
        lines = [
            "=== Grammar Coverage Summary ===",
            f"Total queries generated: {self._total_generated:,}",
            f"Rule alternatives triggered: {covered}/{total} ({self.coverage_rate():.1f}%)",
            f"Errors - known (ignored): {self._errors_known}",
            f"Errors - unknown (logged): {self._errors_unknown}",
        ]
        uncov = self.uncovered()
        if uncov:
            lines.append(f"Uncovered ({len(uncov)}):")
            for rule, idx in sorted(uncov)[:20]:
                lines.append(f"  - {rule}[{idx}]")
            if len(uncov) > 20:
                lines.append(f"  ... and {len(uncov) - 20} more")
        return "\n".join(lines)
