"""Demo Python data pipeline for querying BigQuery.

Google Cloud Function called by PubSub trigger to execute cron job tasks.

Usage:
    python -m main

References:
    https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
"""

from __future__ import annotations

from typing import Any

from ciot import pipeline


def main(data: dict, context: Any):
    """Google Cloud Function called by PubSub trigger to execute cron job tasks."""
    return pipeline.main(data, context)


if __name__ == "__main__":
    main(data={"data": "data"}, context=None)
