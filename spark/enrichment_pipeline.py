import os
import sys

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"


import os
import json
import logging
from functools import reduce as functools_reduce
from typing import Dict

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro,to_avro

from config.spark_config import SparkConfig
from schema_normalizer import SchemaNormalizer
from feature_engineer import FeatureEngineer
from broadcast_joins import BroadcastJoinManager
from redis_state_store import RedisStateStore, MetricsCollector, DeadLetterQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





class FraudEnrichmentPipeline:
    """
    Main Enrichments pipeline for fraud detection.
    consumes from tokenised topics:
    Produces to enriched topics:
    """

    def __init__(self):
        self.config=SparkConfig()
        self.spark= None
        self.normalizer = SchemaNormalizer()
        self.feature_engineer = FeatureEngineer()
        self.broadcast_join_manager = BroadcastJoinManager()
        self.redis_store = RedisStateStore()
        self.metrics=MetricsCollector()
        self.dlq=DeadLetterQueue(dlq_topic=self.config.DLQ_TOPIC)

    def create_spark_session(self) -> SparkSession:
        builder=(
            SparkSession.builder
            .appName(self.config.APP_NAME)

            #checkpointing
            .config(
                "spark.sql.streaming.checkpointLocation",
                self.config.CHECKPOINT_BASE
            )
            .config("spark.pyspark.python", "/usr/bin/python3")
            .config("spark.pyspark.driver.python", "/usr/bin/python3")
            .config("spark.python.worker.faulthandler.enabled", "true")
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "5s")
            .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "5")

            #Adaptive Query execution
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")

            #Broadcast join threshold: 100MB
            .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
            .config("spark.sql.broadcastTimeout", "300")

            #backpressure
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.backpressure.initialRate", "50000")

            #serialization
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrationRequired", "false")
            .config("spark.kryo.unsafe", "true")

            #stability
            .config("spark.network.timeout", "800s")
            .config("spark.executor.heartbeatInterval", "60s")

            #metrics
            .config("spark.sql.streaming.metricsEnabled", "true")
        )

        #Add kafka SASL security
        if self.config.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
            builder = builder \
                .config("spark.kafka.security.protocol",
                        self.config.KAFKA_SECURITY_PROTOCOL)\
                .config("spark.kafka.sasl.mechanism",
                        self.config.KAFKA_SASL_MECHANISM)\
                .config("spark.kafka.sasl.jaas.config",
                        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                        )

        return builder.getOrCreate()

    #Kafka Read
    def read_from_kafka(self) -> DataFrame:
        """
        Read from all tokenised topics in one stream
        """

        topic_list = ",".join(self.config.INPUT_TOPICS.values())
        kafka_options={
            "kafka.bootstrap.servers": self.config.KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": topic_list,
            "startingOffsets": self.config.STARTING_OFFSETS,
            "maxOffsetsPerTrigger": str(self.config.MAX_OFFSETS_PER_TRIGGER),
            "minPartitions": "20",
            "failOnDataLoss": "false",
            "kafka.consumer.heartbeat.interval.ms": "3000",
            "kafka.consumer.session.timeout.ms": "30000",
            "kafka.consumer.max.poll.records": "10000",
            "kafka.consumer.fetch.max.bytes": "104857600",
        }

        #Add security options
        if self.config.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
            kafka_options["kafka.security.protocol"] = self.config.KAFKA_SECURITY_PROTOCOL
            kafka_options["kafka.sasl.mechanism"] = self.config.KAFKA_SASL_MECHANISM
            kafka_options["kafka.sasl.jaas.config"] = (
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            )

        raw_stream=self.spark.readStream.format("kafka")
        for k, v in kafka_options.items():
            raw_stream = raw_stream.option(k,v)

        raw_stream = raw_stream.load()

        #Decode Avro per provider and union all into one DataFrame
        return self._parse_avro_by_topic(raw_stream)

    def _load_avro_schema(self,provider: str) -> str:
        """
        Load Avro Schema file for a provider and return as JSON string

        """

        schema_path=os.path.join(
            self.config.SCHEMA_DIR,
            f"{provider.lower()}_tokenised.avsc"
        )

        try:
            with open(schema_path, "r") as f:
                return json.dumps(json.load(f))

        except FileNotFoundError:
            logger.warning(
                f"Schema file not found: {schema_path}"
            )
            return None

    def _parse_avro_by_topic(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.avro.functions import from_avro
        from pyspark.sql.types import StringType, DoubleType, LongType, BooleanType
        from functools import reduce as functools_reduce

        # ── FIELD MAP goes here ──────────────────────────────────────────────────
        FIELD_MAP = {
            "VISA": {
                "transaction_id": "transaction_id",
                "timestamp": "timestamp",
                "event_time_ms": "event_time_ms",
                "tokenisation_timestamp": "tokenisation_timestamp",
                "tokenisation_method": "tokenisation_method",
                "token_from_cache": "token_from_cache",
                "amount": "amount",
                "currency": "currency",
                "currency_code": "currency_code",
                "source": "source",
                "message_type": "message_type",
                "transaction_type": "transaction_type",
                "card_token": "card_token",
                "bin": "bin",
                "last4": "last4",
                "pos_entry_mode": "pos_entry_mode",
                "merchant_id": "merchant_id",
                "merchant_name": "merchant_name",
                "terminal_id": "terminal_id",
                "mcc": "mcc",
                "acquirer_id": "acquirer_id",
                "stan": "stan",
                "msisdn_token": None,
                "account_token": None,
                "bill_ref_number": None,
                "invoice_number": None,
                "transaction_receipt": None,
                "mpesa_receipt_number": None,
                "first_name": None,
                "last_name": None,
                "result_code": None,
                "result_desc": None,
                "transaction_role": None,
                "third_party_trans_id": None,
                "merchant_request_id": None,
                "checkout_request_id": None,
                "device_fingerprint": None,
                "auth_model": None,
                "status": None,
                "source_type": None,
            },
            "MASTERCARD": {
                "transaction_id": "transaction_id",
                "timestamp": "timestamp",
                "event_time_ms": "event_time_ms",
                "tokenisation_timestamp": "tokenisation_timestamp",
                "tokenisation_method": "tokenisation_method",
                "token_from_cache": "token_from_cache",
                "amount": "amount",
                "currency": "currency",
                "currency_code": "currency_code",
                "source": "source",
                "source_type": "source_type",
                "message_type": "message_type",
                "transaction_type": "transaction_type",
                "card_token": "card_token",
                "bin": "bin",
                "last4": "last4",
                "pos_entry_mode": "pos_entry_mode",
                "merchant_id": "merchant_id",
                "merchant_name": "merchant_name",
                "terminal_id": "terminal_id",
                "mcc": "mcc",
                "acquirer_id": "acquirer_id",
                "stan": "stan",
                "msisdn_token": None,
                "account_token": None,
                "bill_ref_number": None,
                "invoice_number": None,
                "transaction_receipt": None,
                "mpesa_receipt_number": None,
                "first_name": None,
                "last_name": None,
                "result_code": None,
                "result_desc": None,
                "transaction_role": None,
                "third_party_trans_id": None,
                "merchant_request_id": None,
                "checkout_request_id": None,
                "device_fingerprint": None,
                "auth_model": None,
                "status": None,
            },
            "MPESA": {
                "transaction_id": "transaction_id",
                "timestamp": "timestamp",
                "event_time_ms": "event_time_ms",
                "tokenisation_timestamp": "tokenisation_timestamp",
                "tokenisation_method": "tokenisation_method",
                "token_from_cache": "token_from_cache",
                "amount": "amount",
                "currency": "currency",
                "source": "source",
                "source_type": "source_type",
                "transaction_type": "transaction_type",
                "card_token": "msisdn_token",
                "msisdn_token": "msisdn_token",
                "account_token": "account_token",
                "bin": None,
                "last4": None,
                "pos_entry_mode": None,
                "merchant_id": "account_token",
                "merchant_name": None,
                "terminal_id": None,
                "mcc": None,
                "acquirer_id": None,
                "bill_ref_number": "bill_ref_number",
                "invoice_number": "invoice_number",
                "transaction_receipt": "transaction_receipt",
                "mpesa_receipt_number": "mpesa_receipt_number",
                "first_name": "first_name",
                "last_name": "last_name",
                "result_code": "result_code",
                "result_desc": "result_desc",
                "transaction_role": "transaction_role",
                "third_party_trans_id": "third_party_trans_id",
                "merchant_request_id": "merchant_request_id",
                "checkout_request_id": "checkout_request_id",
                "device_fingerprint": None,
                "auth_model": None,
                "status": None,
                "currency_code": None,
                "message_type": None,
                "stan": None,
            },
            "FLUTTERWAVE": {
                "transaction_id": "transaction_id",
                "timestamp": "timestamp",
                "event_time_ms": "event_time_ms",
                "tokenisation_timestamp": "tokenisation_timestamp",
                "tokenisation_method": "tokenisation_method",
                "token_from_cache": "token_from_cache",
                "amount": "amount",
                "currency": "currency",
                "source": "source",
                "source_type": "source_type",
                "transaction_type": "event",
                "card_token": "card_token",
                "bin": "bin",
                "last4": "last4",
                "merchant_id": None,
                "merchant_name": None,
                "terminal_id": None,
                "mcc": None,
                "acquirer_id": None,
                "pos_entry_mode": None,
                "device_fingerprint": "device_fingerprint",
                "auth_model": "auth_model",
                "status": "status",
                "msisdn_token": None,
                "account_token": None,
                "bill_ref_number": None,
                "invoice_number": None,
                "transaction_receipt": None,
                "mpesa_receipt_number": None,
                "first_name": None,
                "last_name": None,
                "result_code": "processor_response",
                "result_desc": None,
                "transaction_role": None,
                "third_party_trans_id": "tx_ref",
                "merchant_request_id": "flw_ref",
                "checkout_request_id": None,
                "currency_code": None,
                "message_type": None,
                "stan": None,
            },
            "PESAPAL": {
                "transaction_id": "transaction_id",
                "timestamp": "timestamp",
                "event_time_ms": "event_time_ms",
                "tokenisation_timestamp": "tokenisation_timestamp",
                "tokenisation_method": "tokenisation_method",
                "token_from_cache": "token_from_cache",
                "amount": "amount",
                "currency": "currency",
                "source": "source",
                "source_type": "source_type",
                "transaction_type": "payment_status",
                "card_token": "payment_account_token",
                "merchant_id": "merchant_reference_token",
                "merchant_name": None,
                "terminal_id": None,
                "mcc": None,
                "acquirer_id": None,
                "pos_entry_mode": None,
                "bin": None,
                "last4": None,
                "msisdn_token": None,
                "account_token": None,
                "bill_ref_number": None,
                "invoice_number": None,
                "transaction_receipt": None,
                "mpesa_receipt_number": None,
                "first_name": "customer_first_name",
                "last_name": "customer_last_name",
                "result_code": "error_code",
                "result_desc": "error_message",
                "transaction_role": None,
                "third_party_trans_id": "tracking_id",
                "merchant_request_id": "pesapal_payment_id",
                "checkout_request_id": None,
                "device_fingerprint": None,
                "auth_model": None,
                "status": "payment_status",
                "currency_code": None,
                "message_type": None,
                "stan": None,
            },
        }

        parsed_dfs = []

        for provider, topic in self.config.INPUT_TOPICS.items():
            schema_str = self._load_avro_schema(provider)
            provider_df = df.filter(col("topic") == lit(topic))

            if schema_str:
                provider_df = provider_df.withColumn(
                    "parsed", from_avro(col("value"), schema_str)
                )
            else:
                from pyspark.sql.functions import from_json
                from pyspark.sql.types import MapType
                provider_df = provider_df.withColumn(
                    "parsed",
                    from_json(
                        col("value").cast(StringType()),
                        MapType(StringType(), StringType())
                    )
                )

            # ── get_col helper goes here ─────────────────────────────────────────
            fmap = FIELD_MAP[provider]

            def get_col(canonical_name, cast_type=StringType(), _fmap=fmap):
                source_field = _fmap.get(canonical_name)
                if source_field is not None:
                    return col(f"parsed.{source_field}").cast(cast_type).alias(canonical_name)
                else:
                    return lit(None).cast(cast_type).alias(canonical_name)

            # ── common_df select goes here ───────────────────────────────────────
            common_df = provider_df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                lit(provider).alias("provider"),
                get_col("transaction_id"),
                get_col("timestamp"),
                get_col("event_time_ms", LongType()),
                get_col("tokenisation_timestamp"),
                get_col("tokenisation_method"),
                get_col("token_from_cache", BooleanType()),
                get_col("amount", DoubleType()),
                get_col("currency"),
                get_col("source"),
                get_col("source_type"),
                get_col("msisdn_token"),
                get_col("account_token"),
                get_col("bill_ref_number"),
                get_col("invoice_number"),
                get_col("transaction_receipt"),
                get_col("mpesa_receipt_number"),
                get_col("first_name"),
                get_col("last_name"),
                get_col("result_code"),
                get_col("result_desc"),
                get_col("transaction_role"),
                get_col("third_party_trans_id"),
                get_col("merchant_request_id"),
                get_col("checkout_request_id"),
                get_col("transaction_type"),
                get_col("card_token"),
                get_col("bin"),
                get_col("last4"),
                get_col("pos_entry_mode"),
                get_col("merchant_id"),
                get_col("merchant_name"),
                get_col("terminal_id"),
                get_col("mcc"),
                get_col("acquirer_id"),
                get_col("device_fingerprint"),
                get_col("auth_model"),
                get_col("status"),
                lit(None).cast(StringType()).alias("currency_code"),
                lit(None).cast(StringType()).alias("message_type"),
                lit(None).cast(StringType()).alias("stan"),
                lit(None).cast(BooleanType()).alias("is_fraud"),
                lit(None).cast(DoubleType()).alias("risk_score"),
                lit(None).cast(StringType()).alias("fraud_pattern"),
                lit(None).cast(StringType()).alias("schema_version"),
            )

            parsed_dfs.append(common_df)

        # ── union all providers ──────────────────────────────────────────────────
        return functools_reduce(lambda x, y: x.union(y), parsed_dfs)

    #Enrichment Pipeline

    def apply_enrichment(self,df: DataFrame) -> DataFrame:
        """
         Apply all enrichment and feature engineering steps.

        Step order matters — later steps depend on earlier ones:
            1.  Watermark             — required before any windowed aggregation
            2.  Schema normalisation  — unified column names across providers
            3.  Broadcast joins       — BIN, MCC, FX, currency (no Redis)
            4.  Derived risk signals  — uses join results
            5.  Velocity features     — windowed aggregations
            6.  Device intelligence   — Redis per-partition lookup
            7.  Location features     — uses bin_country + currency_region
            8.  Temporal features     — hour, day, weekend flags
            9.  Behavioural features  — amount deviation, velocity patterns
            10. Network features      — CNP, chip, device fingerprint
            11. Merchant features     — high-risk MCC combinations
            12. Composite risk score  — lightweight pre-ML heuristic
            13. Metadata              — enrichment_timestamp, version
        """
        # Step 1: Create event timestamp from epoch milliseconds
        df = df.withColumn(
            "event_timestamp",
            (col("event_time_ms") / 1000).cast("timestamp")
        )

        # Step 2: Watermark for late event handling
        df = df.withWatermark(
            "event_timestamp",
            self.config.WATERMARK_DELAY
)

        #Step 2: Normalize to canonical schema
        # df = self.normalizer.normalize(df)  # Already normalized in _parse_avro_by_topic

        #step 3: Broadcast joins
        df=self.broadcast_join_manager.apply_all_joins(df)

        #step 4
        df = self._add_derived_signals(df)

        # Step 5: Velocity features (windowed aggregations)
        df = self.feature_engineer.compute_velocity_features(df)

        # Step 6: Device intelligence (Redis per-partition)
        # df = self.redis_store.enrich_device_history(df)  

        # Step 7: Location features
        df = self.feature_engineer.compute_location_features(df)

        # Step 8: Temporal features
        df = self.feature_engineer.compute_temporal_features(df)

        # Step 9: Behavioural features
        df = self.feature_engineer.compute_behavioral_features(df)

        # Step 10: Network features
        df = self.feature_engineer.compute_network_features(df)

        # Step 11: Merchant features
        df = self.feature_engineer.compute_merchant_features(df)

        # Step 12: Composite risk score
        df = self.feature_engineer.compute_composite_risk_score(df)

        # Step 13: Enrichment metadata
        df = df \
            .withColumn("enrichment_timestamp", current_timestamp()) \
            .withColumn("enrichment_version", lit("1.0"))

        return df

    def _add_derived_signals(self,df: DataFrame) -> DataFrame:
        """Derive boolean risk signals from normalised + joined fields"""

        #International transactions
        df=df.withColumn(
            "is_international",
            when(
                col("bin_country").isNotNull() & col("currency_region").isNotNull(),
                col("bin_country") != col("currency_region"),
            ).otherwise(lit(False)),
        )

        #High Value (> $2,000 USD equvalent)
        df = df.withColumn(
            "is_high_value",
            coalesce(col("amount_usd"),col('amount'))>self.config.HIGH_RISK_THRESHOLD,
        )

        #Card not present
        df = df.withColumn(
            "is_cnp",
            col("pos_entry_mode").isin("01","10","81") |
            coalesce(col("auth_model"),lit("")).isin('NOAUTH',"VBV","3DS") |
            lit(False)
        )

        #Unusual hour (1am-4am UTC)
        from pyspark.sql.functions import hour
        df = df.withColumn(
            "is_unusual_hour",
            hour(col('event_timestamp')).between(1,4)
        )
        # Weekend
        from pyspark.sql.functions import dayofweek
        df = df.withColumn(
            "is_weekend",
            dayofweek(col("event_timestamp")).isin(1, 7),
        )

        # Cross-border currency
        df = df.withColumn(
            "is_cross_border",
            coalesce(col("currency"), lit("USD")) != lit("USD"),
        )

        # Placeholder for rapid succession — refined by velocity step
        df = df.withColumn("is_rapid_succession", lit(False))

        return df

    #Write to kafka
    def write_to_kafka(self,df: DataFrame):
        """
        Write enriched events to provider-specific Kafka topics.
        """

        config = self.config

        def write_batch(batch_df: DataFrame, epoch_id: int):
            # Cache once — avoids recomputing the batch for each provider filter
            batch_df.cache()
            try:
                for provider, topic in config.OUTPUT_TOPICS.items():
                    provider_df = batch_df.filter(col("provider") == provider)

                    # Serialise to JSON (more portable than Avro for initial build)
                    # Switch to to_avro() once Schema Registry is configured
                    output_df = provider_df.select(
                        coalesce(col("card_token"), col("transaction_id")).alias("key"),
                        to_json(struct(*[
                            c for c in provider_df.columns
                            if c not in ("key",)
                        ])).alias("value"),
                    )

                    kafka_write_options = {
                        "kafka.bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                        "topic": topic,
                        "kafka.compression.type": "snappy",
                        "kafka.acks": "all",
                        "kafka.enable.idempotence": "true",
                    }

                    if config.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
                        kafka_write_options.update({
                            "kafka.security.protocol": config.KAFKA_SECURITY_PROTOCOL,
                            "kafka.sasl.mechanism": config.KAFKA_SASL_MECHANISM,
                            "kafka.sasl.jaas.config": (
                                f'org.apache.kafka.common.security.plain.PlainLoginModule '
                                f'required username="{config.KAFKA_API_KEY}" '
                                f'password="{config.KAFKA_API_SECRET}";'
                            ),
                        })

                    output_df.write \
                        .format("kafka") \
                        .options(**kafka_write_options) \
                        .mode("append") \
                        .save()

                    logger.debug(
                        f"Batch {epoch_id}: wrote {provider} → {topic}"
                    )

            except Exception as exc:
                logger.error(f"Batch {epoch_id} write failed: {exc}", exc_info=True)
                # Route failed batch to DLQ
                self.dlq.route_failures(
                    batch_df,
                    config.KAFKA_BOOTSTRAP_SERVERS,
                    error_reason="BATCH_WRITE_FAILURE",
                )
            finally:
                batch_df.unpersist()

        return (
            df.writeStream
            .foreachBatch(write_batch)
            .outputMode("append")
            .trigger(processingTime=self.config.TRIGGER_INTERVAL)
            .option("checkpointLocation",
                    f"{self.config.CHECKPOINT_BASE}/kafka-output")
            .start()
        )

    #Pipeline Execution
    def run(self):
        logger.info("Fraud Detection Enrichment Pipeline — starting")
        # Create Spark session
        self.spark = self.create_spark_session()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created")

        # Load broadcast reference tables (BIN, MCC, FX, currency)
        try:
            import redis as redis_lib
            r = redis_lib.Redis(
                host=self.config.REDIS_HOST,
                port=self.config.REDIS_PORT,
                password=self.config.REDIS_PASSWORD,
                ssl=self.config.REDIS_SSL,
                socket_timeout=0.005,
                decode_responses=True,
            )
            r.ping()
            redis_client = r
            logger.info("Redis connected for FX rate loading")
        except Exception as exc:
            logger.warning(f"Redis unavailable: {exc} — using fallback FX rates")
            redis_client = None

        self.broadcast_join_manager.initialize(self.spark, redis_client)
        logger.info("Broadcast reference tables loaded")

        # Initialise Redis state store
        self.redis_store.initialize(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            password=self.config.REDIS_PASSWORD,
            ssl=self.config.REDIS_SSL,
        )
        logger.info("Redis state store initialised")

        try:
            # Read tokenised events from Kafka
            raw_stream = self.read_from_kafka()
            logger.info("Kafka stream opened")

            # Apply enrichment + feature engineering
            enriched_stream = self.apply_enrichment(raw_stream)
            logger.info("Enrichment pipeline configured")

            # Add pipeline metrics
            enriched_stream = self.metrics.add_pipeline_metrics(enriched_stream)

            # Write enriched records to per-provider Kafka topics
            kafka_query = self.write_to_kafka(enriched_stream)
            logger.info("Kafka write query started")

            # Write metrics (separate query, separate checkpoint)
            #metrics_query = self.metrics.write_to_prometheus(enriched_stream)
            #logger.info("Metrics query started")

            # Log active queries
            logger.info("Active streaming queries:")
            for query in self.spark.streams.active:
                logger.info(f"  {query.name or 'unnamed'} — id={query.id}")

            # Block until any query terminates (or exception)
            self.spark.streams.awaitAnyTermination()

        except Exception as exc:
            logger.error(f"Pipeline failed: {exc}", exc_info=True)
            raise

        finally:
            self.redis_store.close()
            logger.info("Pipeline shutdown complete")

if __name__ == "__main__":
    pipeline = FraudEnrichmentPipeline()
    pipeline.run()

