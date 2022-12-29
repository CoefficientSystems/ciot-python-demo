"""Demo Python data pipeline for querying BigQuery.

Google Cloud Function called by PubSub trigger to execute cron job tasks.

Usage:
    python -m ciot.pipeline

References:
    https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
"""

from __future__ import annotations

import datetime
import logging
from string import Template
from typing import Any

from google.cloud import bigquery

PROJECT_ID = "glass-370516"
DATASET_ID = "finance"
OUTPUT_TABLE_NAME = "new_table"
SQL_QUERY = """
    SELECT *
    FROM `glass-370516.finance.vat_analysis`
    WHERE date < "2022-10-01"
        AND tax_amount > 0
        AND invoice_calculated_tax_rate != 0.2
    ORDER BY date ASC;
    """


def execute_query(query, client, project_id, dataset_id, destination_table):
    """Execute query into destination table."""
    # Get references to the dataset & table
    dataset = client.get_dataset(
        bigquery.DatasetReference(project=project_id, dataset_id=dataset_id)
    )
    table = dataset.table(destination_table)
    # Construct job config
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table
    job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
    # Execute Query
    logging.info("Attempting query...")
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Waits for the query to finish
    logging.info("Query complete. The table is updated.")


def main(data: dict, context: Any):  # pylint: disable=unused-argument
    """Triggered from a message on a Cloud Pub/Sub topic.

    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    # Construct a BigQuery client object.
    client = bigquery.Client()

    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template("Cloud Function was triggered on $time")
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            execute_query(
                query=SQL_QUERY,
                client=client,
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                destination_table=OUTPUT_TABLE_NAME,
            )

        except Exception as error:
            log_message = Template("Query failed due to $message.")
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template("$error").substitute(error=error)
        logging.error(log_message)


if __name__ == "__main__":
    main(data={"data": "data"}, context=None)
