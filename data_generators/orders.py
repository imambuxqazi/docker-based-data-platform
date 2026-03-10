"""PostgreSQL orders data generator.

This generator seeds and continuously mutates a transactional source model:
customers, products, orders, and order_items.

It simulates:
- new customer creation
- new order inserts
- bulk order item inserts
- status progression updates for existing orders
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

import psycopg2
from faker import Faker
from pydantic import BaseModel, Field
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_values
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import AppSettings, get_settings
from data_generators.base import BaseGenerator


LOGGER = structlog.get_logger(__name__)

ORDER_STATUS_FLOW: dict[str, str] = {
    "pending": "processing",
    "processing": "shipped",
    "shipped": "delivered",
}


class CustomerRecord(BaseModel):
    """Customer payload for batch insert."""

    first_name: str
    last_name: str
    email: str
    country_code: str
    city: str


class ProductRecord(BaseModel):
    """Product payload for batch insert."""

    product_sku: str
    product_name: str
    category_name: str
    unit_price: Decimal
    is_active: bool = True


class OrderRecord(BaseModel):
    """Order payload for batch insert."""

    customer_id: int
    order_status: str
    order_timestamp: datetime
    total_amount: Decimal
    currency_code: str = "USD"


class OrderItemRecord(BaseModel):
    """Order item payload for batch insert."""

    order_id: int
    product_id: int
    quantity: int
    unit_price: Decimal


class OrdersGeneratorConfig(BaseModel):
    """Runtime configuration for the orders generator."""

    initial_customer_count: int = Field(default=200)
    initial_product_count: int = Field(default=100)
    new_orders_per_batch_min: int = Field(default=25)
    new_orders_per_batch_max: int = Field(default=60)
    items_per_order_min: int = Field(default=1)
    items_per_order_max: int = Field(default=4)
    status_update_fraction: float = Field(default=0.35)
    currency_code: str = Field(default="USD")
    execute_values_page_size: int = Field(default=1000)


class OrdersGenerator(BaseGenerator):
    """Generate and update transactional order data in PostgreSQL."""

    def __init__(
        self,
        settings: AppSettings | None = None,
        config: OrdersGeneratorConfig | None = None,
    ) -> None:
        """Initialize the orders generator.

        Args:
            settings: Optional application settings.
            config: Optional generator-specific configuration.
        """
        super().__init__(generator_name="orders_generator", settings=settings)
        self.config = config or OrdersGeneratorConfig()
        self.fake = Faker()
        self.random = random.Random(42)
        self.connection_pool: ThreadedConnectionPool | None = None
        self.logger = LOGGER.bind(component="orders_generator")

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(psycopg2.Error),
    )
    def setup(self) -> None:
        """Create the PostgreSQL connection pool and seed base dimensions."""
        postgres_settings = self.settings.postgres

        self.connection_pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=postgres_settings.host,
            port=postgres_settings.port,
            dbname=postgres_settings.database,
            user=postgres_settings.user,
            password=postgres_settings.password,
        )

        self.logger.info("connection_pool_created")
        self._seed_customers_if_needed()
        self._seed_products_if_needed()

    def _get_connection(self) -> psycopg2.extensions.connection:
        """Borrow a connection from the pool.

        Returns:
            An active psycopg2 connection.

        Raises:
            RuntimeError: If the connection pool is not initialized.
        """
        if self.connection_pool is None:
            raise RuntimeError("Connection pool is not initialized.")
        return self.connection_pool.getconn()

    def _put_connection(self, connection: psycopg2.extensions.connection) -> None:
        """Return a borrowed connection to the pool.

        Args:
            connection: Connection to return.
        """
        if self.connection_pool is not None:
            self.connection_pool.putconn(connection)

    def _generate_customers(self, count: int) -> list[CustomerRecord]:
        """Generate customer records.

        Args:
            count: Number of customers to create.

        Returns:
            Customer payload list.
        """
        records: list[CustomerRecord] = []
        for _ in range(count):
            profile = self.fake.simple_profile()
            first_name = profile["name"].split()[0]
            last_name = profile["name"].split()[-1]
            records.append(
                CustomerRecord(
                    first_name=first_name,
                    last_name=last_name,
                    email=self.fake.unique.email(),
                    country_code=self.fake.country_code(representation="alpha-2"),
                    city=self.fake.city(),
                )
            )
        return records

    def _generate_products(self, count: int) -> list[ProductRecord]:
        """Generate product records.

        Args:
            count: Number of products to create.

        Returns:
            Product payload list.
        """
        categories = ["Electronics", "Accessories", "Home", "Sports", "Books"]
        adjectives = ["Premium", "Portable", "Smart", "Compact", "Wireless", "Pro"]
        nouns = ["Speaker", "Laptop", "Bottle", "Headset", "Tracker", "Light", "Mouse"]

        records: list[ProductRecord] = []
        for index in range(count):
            unit_price = Decimal(str(round(self.random.uniform(10, 500), 2))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            records.append(
                ProductRecord(
                    product_sku=f"SKU-{index + 1:05d}",
                    product_name=f"{self.random.choice(adjectives)} {self.random.choice(nouns)}",
                    category_name=self.random.choice(categories),
                    unit_price=unit_price,
                    is_active=True,
                )
            )
        return records

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(psycopg2.Error),
    )
    def _seed_customers_if_needed(self) -> None:
        """Seed the customers table if it is empty."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM customers;")
                customer_count = int(cursor.fetchone()[0])

                if customer_count > 0:
                    self.logger.info("customers_already_seeded", existing_count=customer_count)
                    connection.commit()
                    return

                records = self._generate_customers(self.config.initial_customer_count)
                execute_values(
                    cursor,
                    """
                    INSERT INTO customers (
                        first_name,
                        last_name,
                        email,
                        country_code,
                        city
                    ) VALUES %s
                    """,
                    [
                        (
                            record.first_name,
                            record.last_name,
                            record.email,
                            record.country_code,
                            record.city,
                        )
                        for record in records
                    ],
                    page_size=self.config.execute_values_page_size,
                )
                connection.commit()
                self.logger.info("customers_seeded", inserted_count=len(records))
        finally:
            self._put_connection(connection)

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(psycopg2.Error),
    )
    def _seed_products_if_needed(self) -> None:
        """Seed the products table if it is empty."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM products;")
                product_count = int(cursor.fetchone()[0])

                if product_count > 0:
                    self.logger.info("products_already_seeded", existing_count=product_count)
                    connection.commit()
                    return

                records = self._generate_products(self.config.initial_product_count)
                execute_values(
                    cursor,
                    """
                    INSERT INTO products (
                        product_sku,
                        product_name,
                        category_name,
                        unit_price,
                        is_active
                    ) VALUES %s
                    """,
                    [
                        (
                            record.product_sku,
                            record.product_name,
                            record.category_name,
                            record.unit_price,
                            record.is_active,
                        )
                        for record in records
                    ],
                    page_size=self.config.execute_values_page_size,
                )
                connection.commit()
                self.logger.info("products_seeded", inserted_count=len(records))
        finally:
            self._put_connection(connection)

    def _fetch_ids(self, table_name: str, id_column: str) -> list[int]:
        """Fetch all IDs from a table.

        Args:
            table_name: Source table name.
            id_column: ID column name.

        Returns:
            List of integer IDs.
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT {id_column} FROM {table_name};")
                return [int(row[0]) for row in cursor.fetchall()]
        finally:
            self._put_connection(connection)

    def _choose_random_ids(self, values: list[int], count: int) -> list[int]:
        """Choose random IDs with replacement.

        Args:
            values: Candidate IDs.
            count: Number of IDs to return.

        Returns:
            Randomly selected IDs.
        """
        return [self.random.choice(values) for _ in range(count)]

    def _build_order_records(self, customer_ids: list[int], order_count: int) -> list[OrderRecord]:
        """Create order payloads for a batch.

        Args:
            customer_ids: Available customer IDs.
            order_count: Number of orders to create.

        Returns:
            Batch order payloads with temporary zero totals.
        """
        selected_customer_ids = self._choose_random_ids(customer_ids, order_count)
        current_time = datetime.now(timezone.utc)

        return [
            OrderRecord(
                customer_id=customer_id,
                order_status="pending",
                order_timestamp=current_time,
                total_amount=Decimal("0.00"),
                currency_code=self.config.currency_code,
            )
            for customer_id in selected_customer_ids
        ]

    def _build_order_items_payload(
        self,
        order_ids: list[int],
        product_price_map: dict[int, Decimal],
    ) -> tuple[list[OrderItemRecord], dict[int, Decimal]]:
        """Create order items and corresponding order totals.

        Args:
            order_ids: Newly inserted order IDs.
            product_price_map: Product price lookup.

        Returns:
            Tuple of order item records and total amount per order.
        """
        product_ids = list(product_price_map.keys())
        order_items: list[OrderItemRecord] = []
        totals_by_order: dict[int, Decimal] = {}

        for order_id in order_ids:
            item_count = self.random.randint(
                self.config.items_per_order_min,
                self.config.items_per_order_max,
            )
            selected_product_ids = self.random.sample(product_ids, k=item_count)

            running_total = Decimal("0.00")
            for product_id in selected_product_ids:
                quantity = self.random.randint(1, 3)
                unit_price = product_price_map[product_id]
                line_total = Decimal(quantity) * unit_price
                running_total += line_total

                order_items.append(
                    OrderItemRecord(
                        order_id=order_id,
                        product_id=product_id,
                        quantity=quantity,
                        unit_price=unit_price,
                    )
                )

            totals_by_order[order_id] = running_total.quantize(Decimal("0.01"))

        return order_items, totals_by_order

    def _fetch_product_price_map(self) -> dict[int, Decimal]:
        """Fetch active product prices.

        Returns:
            Mapping of product_id to unit_price.
        """
        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT product_id, unit_price
                    FROM products
                    WHERE is_active = TRUE;
                    """
                )
                rows = cursor.fetchall()
                return {
                    int(row["product_id"]): Decimal(str(row["unit_price"]))
                    for row in rows
                }
        finally:
            self._put_connection(connection)

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(psycopg2.Error),
    )
    def _insert_orders_and_items(self, order_count: int) -> int:
        """Insert a batch of new orders and order items.

        Args:
            order_count: Number of new orders to create.

        Returns:
            Total inserted rows across orders and order items.
        """
        customer_ids = self._fetch_ids("customers", "customer_id")
        product_price_map = self._fetch_product_price_map()

        if not customer_ids or not product_price_map:
            self.logger.warning(
                "insufficient_seed_data",
                customer_count=len(customer_ids),
                product_count=len(product_price_map),
            )
            return 0

        order_records = self._build_order_records(customer_ids, order_count)
        connection = self._get_connection()

        try:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO orders (
                        customer_id,
                        order_status,
                        order_timestamp,
                        total_amount,
                        currency_code
                    ) VALUES %s
                    RETURNING order_id
                    """,
                    [
                        (
                            record.customer_id,
                            record.order_status,
                            record.order_timestamp,
                            record.total_amount,
                            record.currency_code,
                        )
                        for record in order_records
                    ],
                    page_size=self.config.execute_values_page_size,
                )
                order_ids = [int(row[0]) for row in cursor.fetchall()]

                order_item_records, totals_by_order = self._build_order_items_payload(
                    order_ids=order_ids,
                    product_price_map=product_price_map,
                )

                execute_values(
                    cursor,
                    """
                    INSERT INTO order_items (
                        order_id,
                        product_id,
                        quantity,
                        unit_price
                    ) VALUES %s
                    """,
                    [
                        (
                            item.order_id,
                            item.product_id,
                            item.quantity,
                            item.unit_price,
                        )
                        for item in order_item_records
                    ],
                    page_size=self.config.execute_values_page_size,
                )

                execute_values(
                    cursor,
                    """
                    UPDATE orders AS target
                    SET total_amount = source.total_amount
                    FROM (VALUES %s) AS source(order_id, total_amount)
                    WHERE target.order_id = source.order_id::BIGINT
                    """,
                    [
                        (order_id, total_amount)
                        for order_id, total_amount in totals_by_order.items()
                    ],
                    page_size=self.config.execute_values_page_size,
                    template="(%s, %s)",
                )

                connection.commit()

                inserted_count = len(order_ids) + len(order_item_records)
                self.logger.info(
                    "orders_batch_inserted",
                    orders_inserted=len(order_ids),
                    order_items_inserted=len(order_item_records),
                    rows_generated=inserted_count,
                )
                return inserted_count
        except Exception:
            connection.rollback()
            raise
        finally:
            self._put_connection(connection)

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(psycopg2.Error),
    )
    def _progress_existing_orders(self) -> int:
        """Advance a subset of existing orders through the status pipeline.

        Returns:
            Number of updated rows.
        """
        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT order_id, order_status
                    FROM orders
                    WHERE order_status IN ('pending', 'processing', 'shipped');
                    """
                )
                open_orders = cursor.fetchall()

                if not open_orders:
                    connection.commit()
                    return 0

                update_count = max(1, int(len(open_orders) * self.config.status_update_fraction))
                selected_rows = self.random.sample(open_orders, k=min(update_count, len(open_orders)))

                updates = [
                    (int(row["order_id"]), ORDER_STATUS_FLOW[str(row["order_status"])])
                    for row in selected_rows
                ]

                execute_values(
                    cursor,
                    """
                    UPDATE orders AS target
                    SET order_status = source.order_status
                    FROM (VALUES %s) AS source(order_id, order_status)
                    WHERE target.order_id = source.order_id::BIGINT
                    """,
                    updates,
                    page_size=self.config.execute_values_page_size,
                    template="(%s, %s)",
                )
                connection.commit()

                self.logger.info("orders_status_progressed", updated_count=len(updates))
                return len(updates)
        except Exception:
            connection.rollback()
            raise
        finally:
            self._put_connection(connection)

    def generate_batch(self) -> int:
        """Generate one orders batch and a related status-update batch.

        Returns:
            Total affected rows across inserts and updates.
        """
        order_count = self.random.randint(
            self.config.new_orders_per_batch_min,
            self.config.new_orders_per_batch_max,
        )
        inserted_rows = self._insert_orders_and_items(order_count)
        updated_rows = self._progress_existing_orders()

        total_rows = inserted_rows + updated_rows
        time.sleep(self.settings.batch.orders_generator_sleep_seconds)
        return total_rows

    def cleanup(self) -> None:
        """Close the PostgreSQL connection pool."""
        if self.connection_pool is not None:
            self.connection_pool.closeall()
            self.logger.info("connection_pool_closed")


if __name__ == "__main__":
    OrdersGenerator(settings=get_settings()).run()