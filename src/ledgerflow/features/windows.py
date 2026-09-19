"""Window definitions -- the single source of truth for online and offline.

This module is imported by the streaming risk worker AND read by the Spark
jobs, so there is exactly one place that says how long "1h spend" is and how
late an event may arrive.

Two implementations of the same window logic drift. The drift does not announce
itself: it shows up months later as a model that scored well in backtest and
fails in production, and tracking it down means diffing two codebases written
at different times by people who each thought theirs was right. One definition
cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from ..config import settings


@dataclass(frozen=True, slots=True)
class WindowSpec:
    name: str
    duration: timedelta
    slide: timedelta | None = None

    @property
    def spark_window(self) -> tuple[str, str | None]:
        """Spark's ``window()`` takes strings; derive them, never retype them."""
        def fmt(td: timedelta) -> str:
            total = int(td.total_seconds())
            if total % 86400 == 0:
                return f"{total // 86400} days"
            if total % 3600 == 0:
                return f"{total // 3600} hours"
            return f"{total // 60} minutes"

        return fmt(self.duration), (fmt(self.slide) if self.slide else None)

    def bounds(self, at: datetime) -> tuple[datetime, datetime]:
        return at - self.duration, at


SPEND_1H = WindowSpec("spend_1h", timedelta(hours=1), timedelta(minutes=5))
SPEND_24H = WindowSpec("spend_24h", timedelta(hours=24), timedelta(minutes=30))
MERCHANTS_7D = WindowSpec("distinct_merchants_7d", timedelta(days=7), timedelta(hours=1))
BASELINE_90D = WindowSpec("baseline_90d", timedelta(days=90))

ALL = (SPEND_1H, SPEND_24H, MERCHANTS_7D, BASELINE_90D)

#: How late an event may arrive and still count toward its window.
#: Spark's ``withWatermark`` gets this exact value.
WATERMARK = timedelta(seconds=settings.watermark_seconds)


def watermark_for(max_event_time: datetime) -> datetime:
    """The watermark: the newest event time seen, minus the allowed lateness.

    Anything older than this is late. Late events are dropped from the
    aggregate -- which is correct -- and recorded in ``late_arrivals``, which is
    what makes the drop observable instead of silent.
    """
    return max_event_time - WATERMARK
