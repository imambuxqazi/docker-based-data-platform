# Data Platform Run Instructions

## Initial bootstrap (one time)

### Start the platform
    docker compose up -d

### Verify Services are up
    docker compose ps

## Note: Data Generation scripts are not part of DAG so run them before the Airflow DAG run
### Generate initial source data

### CDC Orders Source:
    docker exec data_platform_airflow_webserver python -c "from config.settings import get_settings; from data_generators.orders import OrdersGenerator; g = OrdersGenerator(settings=get_settings()); g.setup(); print(g.generate_batch()); g.cleanup()"

### Clickstream Source:
    docker exec data_platform_airflow_webserver python -c "from config.settings import get_settings; from data_generators.clickstream import ClickstreamGenerator; g = ClickstreamGenerator(settings=get_settings()); g.setup(); print(g.generate_batch()); g.cleanup()"


### Because DAGs cannot process data if no source data exists yet.

    Enable DAGs

    Open Airflow UI:
    http:localhost:8089

    Enable:
    orders_pipeline
    clickstream_pipeline

    Airflow will now run them automatically.

    Schedules defined:

    DAG                  | Schedule         
    orders_pipeline      | every 15 minutes
    clickstream_pipeline | every 5 minutes 



### What Airflow Does Automatically
    Orders DAG

    check_postgres_health
            ↓
    poll_and_ingest (Bronze)
            ↓
    process_silver
            ↓
    process_gold
            ↓
    run_quality
            ↓
    notify_on_failure


    Clickstream DAG

    check_redpanda_health
            ↓
    process_silver
            ↓
    process_gold
            ↓
    run_quality
            ↓
    notify_on_failure

### What You Should Do During Operation

Once DAGs are enabled:

You only need to do two things.

Keep generating source data

Simulate new system activity.

    Example:

    docker exec data_platform_airflow_webserver python -c "from config.settings import get_settings; from data_generators.orders import OrdersGenerator; g = OrdersGenerator(settings=get_settings()); g.setup(); print(g.generate_batch()); g.cleanup()"

    And:

    docker exec data_platform_airflow_webserver python -c "from config.settings import get_settings; from data_generators.clickstream import ClickstreamGenerator; g = ClickstreamGenerator(settings=get_settings()); g.setup(); print(g.generate_batch()); g.cleanup()"

    This simulates:

    new orders
    new events
    order status updates

## Query Gold tables to verify results

    Run analytics queries

    Example conversion funnel query:

    docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from analytics.queries import conversion_funnel; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('analytics_run'); today = datetime.now(timezone.utc).strftime('%Y-%m-%d'); conversion_funnel(spark, settings, today, today); spark.stop()"

    Example hourly traffic query:

    docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from analytics.queries import hourly_traffic; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('analytics_hourly_run'); today = datetime.now(timezone.utc).strftime('%Y-%m-%d'); hourly_traffic(spark, settings, today); spark.stop()"

    Run top revenue products

    docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from analytics.queries import top_revenue_products; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('analytics_top_products'); today = datetime.now(timezone.utc).strftime('%Y-%m-%d'); result = top_revenue_products(spark, settings, today, today, top_n=10); print(result.to_dict(orient='records')); spark.stop()"

    Run customer segments

    docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from analytics.queries import customer_segments; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('analytics_segments'); today = datetime.now(timezone.utc).strftime('%Y-%m-%d'); result = customer_segments(spark, settings, today); print(result.to_dict(orient='records')); spark.stop()"

    Run cart abandonment

    docker exec data_platform_airflow_webserver python -c "from datetime import datetime, timezone; from config.settings import get_settings; from config.spark import SparkSessionBuilder; from analytics.queries import cart_abandonment; settings = get_settings(); spark = SparkSessionBuilder(settings=settings).build('analytics_cart_abandonment'); today = datetime.now(timezone.utc).strftime('%Y-%m-%d'); result = cart_abandonment(spark, settings, today, today); print(result.to_dict(orient='records')); spark.stop()"