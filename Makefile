.PHONY: help up down restart ps health logs \
	generate-orders generate-clicks \
	run-orders-pipeline run-clickstream-pipeline \
	run-analytics run-quality test \
	airflow-ui minio-ui spark-ui redpanda-ui clean

help:
	@echo.
	@echo Available commands:
	@echo   make up                      - Start the full platform
	@echo   make down                    - Stop the full platform
	@echo   make restart                 - Restart the full platform
	@echo   make ps                      - Show container status
	@echo   make health                  - Run core health checks
	@echo   make logs                    - Tail all docker compose logs
	@echo   make generate-orders         - Run one orders generator batch
	@echo   make generate-clicks         - Run one clickstream generator batch
	@echo   make run-orders-pipeline     - Run one Bronze orders ingestion cycle + Silver + Gold
	@echo   make run-clickstream-pipeline- Run Silver + Gold clickstream processing for today
	@echo   make run-analytics           - Execute analytics queries for today
	@echo   make run-quality             - Run sample quality checks
	@echo   make test                    - Run pytest suite inside Docker
	@echo   make airflow-ui              - Print Airflow UI URL
	@echo   make minio-ui                - Print MinIO UI URL
	@echo   make spark-ui                - Print Spark UI URL
	@echo   make redpanda-ui             - Print Redpanda UI URL
	@echo   make clean                   - Stop stack and remove volumes
	@echo.

up:
	docker compose up -d

down:
	docker compose down

restart:
	cmd /c "docker compose down && docker compose up -d"

ps:
	docker compose ps

health:
	cmd /c "docker exec data_platform_postgres pg_isready -U app_user -d source_db && docker exec data_platform_redpanda rpk cluster health && docker exec data_platform_minio curl -f http://localhost:9000/minio/health/live && docker exec data_platform_airflow_webserver curl -f http://localhost:8080/health"

logs:
	docker compose logs -f --tail=200

generate-orders:
	docker exec data_platform_airflow_webserver python -c "from config.settings import get_settings; from data_generators.orders import OrdersGenerator; generator = OrdersGenerator(settings=get_settings()); generator.setup(); rows = generator.generate_batch(); print({'rows_generated': rows}); generator.cleanup()"

generate-clicks:
	docker exec data_platform_airflow_webserver python -c "from config.settings import get_settings; from data_generators.clickstream import ClickstreamGenerator; generator = ClickstreamGenerator(settings=get_settings()); generator.setup(); rows = generator.generate_batch(); print({'events_published': rows}); generator.cleanup()"

run-orders-pipeline:
	docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from pipelines.ingestion.orders import ingest_all_orders_tables; from pipelines.silver.orders import process_orders_silver_partition; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from pipelines.gold.sales import build_daily_sales; from pipelines.gold.segments import build_customer_segments; settings = get_settings(); partition_date = datetime.now(timezone.utc).strftime('%Y-%m-%d'); bronze_result = ingest_all_orders_tables(settings=settings); print({'bronze_result': bronze_result}); silver_result = process_orders_silver_partition(partition_date=partition_date, settings=settings); print({'silver_result': silver_result}); spark = SparkSessionBuilder(settings=settings).build('make_orders_gold'); print({'sales_result': build_daily_sales(spark, settings, partition_date)}); print({'segments_result': build_customer_segments(spark, settings, partition_date)}); spark.stop()"

run-clickstream-pipeline:
	docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from pipelines.silver.clickstream import process_clickstream_silver_partition; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from pipelines.gold.funnel import build_product_funnel; from pipelines.gold.traffic import build_hourly_traffic; from pipelines.gold.alerts import build_inventory_alerts; settings = get_settings(); partition_date = datetime.now(timezone.utc).strftime('%Y-%m-%d'); silver_result = process_clickstream_silver_partition(partition_date=partition_date, settings=settings); print({'silver_result': silver_result}); spark = SparkSessionBuilder(settings=settings).build('make_clickstream_gold'); print({'funnel_result': build_product_funnel(spark, settings, partition_date)}); print({'traffic_result': build_hourly_traffic(spark, settings, partition_date)}); print({'alerts_result': build_inventory_alerts(spark, settings)}); spark.stop()"

run-analytics:
	docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from analytics.queries import top_revenue_products, conversion_funnel, hourly_traffic, customer_segments, cart_abandonment; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('make_analytics'); today = datetime.now(timezone.utc).strftime('%Y-%m-%d'); top_revenue_products(spark, settings, today, today, top_n=10); conversion_funnel(spark, settings, today, today); hourly_traffic(spark, settings, today); customer_segments(spark, settings, today); cart_abandonment(spark, settings, today, today); spark.stop()"

run-quality:
	docker exec data_platform_airflow_webserver python -c "from alerts.manager import AlertManager; from quality.runner import QualityRunner; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('make_quality'); schema = StructType([StructField('id', IntegerType(), True), StructField('category', StringType(), True), StructField('amount', DoubleType(), True)]); df = spark.createDataFrame([(1, 'A', 10.0), (2, 'B', 20.0)], schema=schema); runner = QualityRunner(AlertManager(settings)); report = runner.run_suite(df, 'make_quality_table', [{'check_type': 'row_count', 'min_row_count': 1}, {'check_type': 'null_rate', 'column_name': 'category', 'max_null_rate': 0.1}, {'check_type': 'duplicate_rate', 'key_columns': ['id'], 'max_duplicate_rate': 0.0}, {'check_type': 'value_range', 'column_name': 'amount', 'min_value': 0, 'max_value': 100}]); print(report); spark.stop()"

test:
	docker exec data_platform_airflow_webserver pytest /opt/airflow/tests -q

airflow-ui:
	@echo http://localhost:8089

minio-ui:
	@echo http://localhost:9001

spark-ui:
	@echo http://localhost:8080

redpanda-ui:
	@echo http://localhost:8081

clean:
	docker compose down -v