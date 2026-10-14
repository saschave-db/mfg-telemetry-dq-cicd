"""Bronze de-duplication flows: the only Python in the pipeline. Everything else is SQL (telemetry_pipeline.sql).

Why Python here: SQL has no streaming equivalent of dropDuplicatesWithinWatermark. SELECT DISTINCT with a WATERMARK
is planned as an aggregation and holds the newest rows back until the watermark passes them (2 minutes later, or the
next update of a triggered pipeline). dropDuplicatesWithinWatermark emits every first occurrence immediately.

Zerobus is at-least-once and exports can be re-sent, so each source drops repeats of an event_id within 2 minutes.
One watermark per source: a shared one would let the faster source advance it and drop the other's on-time rows.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()


def _deduplicated(source_view: str):
    return (
        spark.readStream.table(source_view)  # temporary views defined in telemetry_pipeline.sql
        # Rows without an event_id (e.g. a truncated file line) are never duplicates of each other.
        .withColumn("_dedup_key", F.coalesce("event_id", F.expr("uuid()")))
        .withWatermark("_wm_ts", "2 minutes")
        .dropDuplicatesWithinWatermark(["_dedup_key"])
        .drop("_wm_ts", "_dedup_key")
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.append_flow(target="bronze_telemetry", name="bronze_from_zerobus")
def bronze_from_zerobus():
    return _deduplicated("zerobus_source")


@dp.append_flow(target="bronze_telemetry", name="bronze_from_files")
def bronze_from_files():
    return _deduplicated("files_source")
