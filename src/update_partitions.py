import logging
import time

import boto3


DATABASE = "financial_market"

OUTPUT = (
    "s3://samuel-financial-data-lake/"
    "athena-results/"
)

QUERIES = [
    "MSCK REPAIR TABLE asset_ranking;",
    "MSCK REPAIR TABLE market_alerts;",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def create_client():

    return boto3.client("athena")


def run_query(
    athena,
    query,
):

    logger.info(
        f"Executing: {query}"
    )

    response = athena.start_query_execution(

        QueryString=query,

        QueryExecutionContext={
            "Database": DATABASE
        },

        ResultConfiguration={
            "OutputLocation": OUTPUT
        }

    )

    return response["QueryExecutionId"]


def wait_for_query(
    athena,
    query_execution_id,
):

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        state = response[
            "QueryExecution"
        ][
            "Status"
        ][
            "State"
        ]

        if state == "SUCCEEDED":

            logger.info(
                f"Query {query_execution_id} completed successfully."
            )

            return

        if state in (
            "FAILED",
            "CANCELLED",
        ):

            reason = response[
                "QueryExecution"
            ][
                "Status"
            ].get(
                "StateChangeReason",
                "Unknown error"
            )

            raise RuntimeError(
                f"Query failed: {reason}"
            )

        time.sleep(2)


def repair_tables():

    athena = create_client()

    for query in QUERIES:

        query_execution_id = run_query(
            athena,
            query,
        )

        wait_for_query(
            athena,
            query_execution_id,
        )


def main():

    logger.info(
        "Starting Athena metadata refresh..."
    )

    repair_tables()

    logger.info(
        "Athena metadata refresh completed successfully."
    )


if __name__ == "__main__":
    main()