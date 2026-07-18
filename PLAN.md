# Olist E-Commerce Data Lakehouse Implementation Plan

## 1. Project objective

The objective is to implement a Medallion Data Lakehouse for e-commerce sales and customer analytics.

The pipeline will:

1. Ingest the original Olist CSV files.
2. Store immutable source data in a Bronze zone on MinIO.
3. Clean, validate, deduplicate, standardize, and enrich the data in Silver.
4. Create business-oriented dimensional models and aggregated datasets in Gold.
5. Store Silver and Gold datasets as Apache Iceberg tables.
6. Optimize the tables through partitioning, bucketing, sorting, compaction, and metadata maintenance.
7. Make Gold tables available to a BI tool or query engine.

The final platform should answer questions such as:

* How much revenue is generated daily and monthly?
* Which products and categories sell the most?
* Which sellers perform best?
* How long does delivery take?
* Which customers purchase repeatedly?
* Which states generate the most revenue?
* What is the relationship between delivery delays and review scores?
* Which payment methods are preferred?
* What is the average order value?
* What is the cancellation rate?
* Which customers belong to the most valuable RFM segments?

---

# 2. Technology stack

## 2.1 Core stack

| Component                      | Responsibility                                             |
| ------------------------------ | ---------------------------------------------------------- |
| Olist Dataset                  | Source e-commerce data                                     |
| MinIO                          | S3-compatible object storage                               |
| Apache Spark                   | Data ingestion, transformation, validation and aggregation |
| Apache Iceberg                 | Table format for Silver and Gold datasets                  |
| Iceberg REST Catalog or Nessie | Iceberg table catalog                                      |
| PostgreSQL                     | Optional catalog backend or application database           |
| Docker Compose                 | Local development environment                              |
| Airflow                        | Optional orchestration                                     |
| Trino                          | Optional interactive SQL query engine                      |
| Superset, Metabase or Power BI | Dashboard and analytics                                    |
| Great Expectations or Soda     | Optional data-quality validation                           |
| Prometheus and Grafana         | Optional infrastructure monitoring                         |

## 2.2 Why Iceberg is used

Apache Iceberg provides table semantics over files stored in object storage. It supports features such as:

* ACID transactions
* schema evolution
* hidden partitioning
* partition evolution
* snapshot-based reads
* time travel
* atomic table updates
* file-level statistics
* multiple processing engines

Iceberg is designed to allow engines such as Spark, Trino, Flink, Presto and Hive to work with the same analytical tables.

MinIO provides the S3-compatible storage layer, while Iceberg stores table metadata and Spark performs the transformations. Iceberg can access S3-compatible storage through its S3 integration.

---

# 3. High-level architecture

```text
Olist CSV Dataset
        │
        ▼
Dataset ingestion script
        │
        ▼
MinIO Landing Zone
        │
        ▼
Bronze raw files
        │
        ▼
Spark Bronze-to-Silver jobs
        │
        ├── Cleaning
        ├── Type casting
        ├── Validation
        ├── Deduplication
        ├── Standardization
        ├── Data-quality rules
        └── Business enrichment
        │
        ▼
Iceberg Silver Tables on MinIO
        │
        ▼
Spark Silver-to-Gold jobs
        │
        ├── Dimensional modeling
        ├── Fact tables
        ├── Customer analytics
        ├── Sales aggregations
        ├── Delivery KPIs
        └── Marketing-oriented segments
        │
        ▼
Iceberg Gold Tables on MinIO
        │
        ├── Trino / Spark SQL
        ├── Superset / Metabase
        └── Machine-learning notebooks
```

---

# 4. Lakehouse storage organization

Use separate MinIO buckets or prefixes for each responsibility.

## Option A: separate buckets

```text
olist-landing
olist-bronze
olist-silver
olist-gold
olist-checkpoints
olist-logs
```

## Option B: one main bucket with prefixes

```text
s3://olist-lakehouse/
    landing/
    bronze/
    warehouse/
        silver/
        gold/
    checkpoints/
    rejected/
    logs/
```

The second design is simpler for a small project.

Recommended structure:

```text
s3://olist-lakehouse/
├── landing/
│   └── olist/
│       └── load_date=YYYY-MM-DD/
│
├── bronze/
│   └── olist/
│       ├── customers/
│       ├── orders/
│       ├── order_items/
│       ├── products/
│       ├── sellers/
│       ├── payments/
│       ├── reviews/
│       ├── geolocation/
│       └── category_translation/
│
├── warehouse/
│   ├── silver/
│   └── gold/
│
├── checkpoints/
├── rejected/
└── audit/
```

Silver and Gold should be managed as Iceberg tables rather than manually organized Parquet directories.

---

# 5. Lakehouse catalog and schemas

Create one Iceberg catalog and three logical schemas.

```text
olist_catalog
├── bronze
├── silver
├── gold
└── governance
```

## 5.1 Bronze schema

Bronze represents the data as received from the source.

```text
olist_catalog.bronze
```

Possible Bronze tables:

```text
bronze_customers
bronze_orders
bronze_order_items
bronze_products
bronze_sellers
bronze_order_payments
bronze_order_reviews
bronze_geolocation
bronze_product_category_translation
```

For the simplest implementation, Bronze can remain as raw CSV or Parquet files instead of Iceberg tables. However, using Iceberg in Bronze is useful if you want ingestion auditing, schema tracking and incremental replay.

## 5.2 Silver schema

Silver contains clean, validated, standardized and reusable business entities.

```text
olist_catalog.silver
```

Recommended Silver tables:

```text
customers
orders
order_items
products
sellers
payments
reviews
geolocation
product_categories
order_details
```

## 5.3 Gold schema

Gold contains analytics-ready business models.

```text
olist_catalog.gold
```

Recommended Gold groups:

```text
Dimensions
├── dim_date
├── dim_customer
├── dim_product
├── dim_seller
├── dim_geography
└── dim_payment_type

Facts
├── fact_orders
├── fact_order_items
├── fact_payments
└── fact_reviews

Aggregates
├── agg_daily_sales
├── agg_monthly_sales
├── agg_category_performance
├── agg_seller_performance
├── agg_customer_value
├── agg_delivery_performance
├── agg_state_performance
└── agg_payment_performance

Data products
├── customer_rfm
├── customer_360
├── seller_scorecard
├── product_scorecard
└── sales_dashboard
```

## 5.4 Governance schema

```text
olist_catalog.governance
```

Suggested tables:

```text
pipeline_runs
data_quality_results
rejected_records
source_file_registry
table_freshness
row_count_reconciliation
```

---

# 6. Source dataset mapping

The main Olist files are mapped as follows.

| Source file          | Main key                     | Purpose                               |
| -------------------- | ---------------------------- | ------------------------------------- |
| customers            | customer_id                  | Customer and location information     |
| orders               | order_id                     | Order lifecycle and timestamps        |
| order_items          | order_id, order_item_id      | Products, sellers, prices and freight |
| products             | product_id                   | Product characteristics               |
| sellers              | seller_id                    | Seller location                       |
| order_payments       | order_id, payment_sequential | Payment information                   |
| order_reviews        | review_id, order_id          | Satisfaction and review text          |
| geolocation          | zip-code prefix              | Latitude and longitude                |
| category translation | product category             | Portuguese-to-English translation     |

---

# 7. Phase 1 — Environment setup

## 7.1 Local infrastructure

Create a Docker Compose environment containing:

```text
MinIO
Iceberg REST Catalog or Nessie
Spark master
Spark worker
Spark history server
PostgreSQL, if required
Airflow, optional
Trino, optional
Superset, optional
```

## 7.2 Spark configuration

Configure Spark with:

* Iceberg Spark runtime
* Iceberg catalog extension
* MinIO endpoint
* S3 access key and secret key
* path-style access
* warehouse location
* adaptive query execution
* event logging

Conceptual Spark configuration:

```text
spark.sql.extensions=
  org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

spark.sql.catalog.olist=
  org.apache.iceberg.spark.SparkCatalog

spark.sql.catalog.olist.type=rest

spark.sql.catalog.olist.uri=http://iceberg-rest:8181

spark.sql.catalog.olist.warehouse=
  s3://olist-lakehouse/warehouse

spark.sql.catalog.olist.io-impl=
  org.apache.iceberg.aws.s3.S3FileIO

spark.sql.catalog.olist.s3.endpoint=
  http://minio:9000

spark.sql.adaptive.enabled=true
```

Keep package versions compatible across:

* Spark
* Scala
* Iceberg runtime
* Hadoop AWS or Iceberg AWS bundle
* AWS SDK

---

# 8. Phase 2 — Landing and Bronze ingestion

## 8.1 Landing process

Download and extract the source CSV files.

Upload the unmodified files to:

```text
s3://olist-lakehouse/landing/olist/load_date=YYYY-MM-DD/
```

Never edit the original landing files.

## 8.2 Bronze ingestion requirements

For every source file:

1. Read the CSV using a declared schema.
2. Add ingestion metadata.
3. Preserve the original source values.
4. Record the source filename.
5. Record the ingestion timestamp.
6. Calculate a row hash.
7. Store malformed rows separately.
8. Write a pipeline audit record.

Recommended technical columns:

```text
_ingestion_timestamp
_ingestion_date
_source_file
_source_system
_batch_id
_record_hash
_is_valid
_validation_errors
```

## 8.3 Why explicit schemas matter

Avoid relying only on Spark schema inference because:

* identifiers may be interpreted incorrectly;
* numeric values may receive inappropriate types;
* timestamp parsing may vary;
* empty columns can produce unstable schemas;
* repeated executions should produce identical structures.

## 8.4 Bronze validations

Perform basic technical validation only:

* file exists;
* expected columns exist;
* file is not empty;
* file can be parsed;
* row count is recorded;
* duplicate file ingestion is detected;
* malformed rows are isolated.

Do not heavily transform business values in Bronze.

---

# 9. Phase 3 — Silver layer

## 9.1 Purpose of Silver

Silver is the trusted and reusable layer of the Lakehouse.

Its role is to transform raw source data into consistent business entities without creating dashboard-specific aggregations.

Silver operations include:

* renaming columns;
* assigning correct data types;
* standardizing text;
* parsing timestamps;
* validating identifiers;
* deduplicating records;
* treating null values;
* rejecting invalid records;
* applying referential-integrity checks;
* deriving reusable business fields;
* enriching records through joins;
* resolving geolocation duplicates;
* creating incremental upsert logic.

Silver should remain detailed enough to support multiple Gold use cases.

---

# 10. Silver table transformations

## 10.1 `silver.customers`

Source:

```text
bronze_customers
```

Transformations:

* validate `customer_id`;
* validate `customer_unique_id`;
* trim city and state fields;
* convert city names to lowercase or title case;
* normalize accents if required;
* standardize state abbreviations;
* validate ZIP-code prefixes;
* remove exact duplicate records;
* retain one current record per `customer_id`;
* add geographic classification if desired.

Resulting columns:

```text
customer_id
customer_unique_id
customer_zip_code_prefix
customer_city
customer_state
customer_region
record_created_at
record_updated_at
```

Important distinction:

* `customer_id` identifies the customer within a specific order context;
* `customer_unique_id` is more appropriate for customer-level analytics and repeat-purchase analysis.

## 10.2 `silver.orders`

Source:

```text
bronze_orders
```

Transformations:

* cast all timestamp columns;
* validate order status;
* validate `customer_id`;
* deduplicate by `order_id`;
* standardize status values;
* reject impossible timestamp sequences;
* derive purchase date;
* derive approval delay;
* derive carrier handling time;
* derive delivery duration;
* derive estimated delivery error;
* determine whether delivery was late.

Derived columns:

```text
purchase_date
purchase_year
purchase_month
purchase_year_month
purchase_day_of_week
purchase_hour
approval_delay_hours
delivery_duration_days
estimated_delivery_days
delivery_delay_days
is_delivered
is_cancelled
is_late_delivery
```

Example rules:

```text
order_purchase_timestamp <= order_approved_at
order_approved_at <= order_delivered_carrier_date
order_delivered_carrier_date <= order_delivered_customer_date
```

Rows that violate the rules should be flagged rather than silently removed.

## 10.3 `silver.order_items`

Source:

```text
bronze_order_items
```

Transformations:

* validate the composite key;
* cast price and freight values to decimal;
* reject negative prices;
* reject negative freight values;
* validate product and seller identifiers;
* cast shipping-limit timestamp;
* calculate line revenue;
* calculate total line amount.

Derived columns:

```text
gross_item_amount = price
freight_amount = freight_value
total_item_amount = price + freight_value
```

Suggested key:

```text
order_id + order_item_id
```

## 10.4 `silver.products`

Sources:

```text
bronze_products
bronze_product_category_translation
```

Transformations:

* validate `product_id`;
* trim category names;
* join Portuguese category names with English translations;
* fill missing translations with a controlled value;
* cast dimensions and weights;
* reject or flag negative measurements;
* derive product volume;
* derive product size band;
* derive product weight band.

Derived columns:

```text
product_category_name_pt
product_category_name_en
product_volume_cm3
product_weight_band
product_size_band
has_category
has_complete_dimensions
```

Formula:

```text
product_volume_cm3 =
    product_length_cm
  × product_height_cm
  × product_width_cm
```

## 10.5 `silver.sellers`

Transformations:

* validate `seller_id`;
* normalize city;
* validate state;
* validate ZIP prefix;
* remove duplicates;
* enrich with seller region;
* add approximate latitude and longitude if available.

## 10.6 `silver.payments`

Transformations:

* validate `order_id`;
* validate payment sequence;
* normalize payment type;
* cast installment count;
* cast payment value;
* reject negative payment values;
* identify voucher or split-payment orders;
* calculate the number of payment methods per order.

Primary key candidate:

```text
order_id + payment_sequential
```

## 10.7 `silver.reviews`

Transformations:

* validate review and order identifiers;
* cast timestamps;
* validate review score between 1 and 5;
* normalize empty comments to null;
* trim review title and message;
* remove exact duplicate reviews;
* calculate whether the review includes text;
* calculate review-response delay;
* optionally derive sentiment later.

Derived columns:

```text
has_review_title
has_review_message
review_creation_date
review_response_delay_days
review_score_category
```

Possible score categories:

```text
1–2: Negative
3: Neutral
4–5: Positive
```

## 10.8 `silver.geolocation`

Geolocation requires special treatment because a ZIP-code prefix can appear multiple times.

Transformations:

* validate latitude and longitude;
* remove invalid coordinate ranges;
* standardize city and state;
* group records by ZIP-code prefix;
* calculate representative latitude and longitude;
* retain the number of source coordinates.

Recommended aggregation:

```text
GROUP BY geolocation_zip_code_prefix
```

Result:

```text
zip_code_prefix
representative_latitude
representative_longitude
city
state
coordinate_count
```

Median coordinates can be more robust than a simple mean when outliers exist.

## 10.9 `silver.order_details`

This is an optional wide Silver table created for convenient downstream use.

It can join:

```text
orders
    + order_items
    + customers
    + products
    + sellers
```

Grain:

```text
One row per order item
```

It should not join payments directly without aggregation because an order can have multiple items and multiple payments. Joining both directly can create a many-to-many multiplication problem.

Before joining payments, create:

```text
silver.payment_summary_by_order
```

Before joining reviews, create:

```text
silver.review_summary_by_order
```

---

# 11. Silver data-quality rules

## 11.1 Uniqueness rules

```text
customers.customer_id must be unique
orders.order_id must be unique
products.product_id must be unique
sellers.seller_id must be unique
order_items(order_id, order_item_id) must be unique
payments(order_id, payment_sequential) must be unique
```

## 11.2 Referential-integrity rules

```text
orders.customer_id exists in customers
order_items.order_id exists in orders
order_items.product_id exists in products
order_items.seller_id exists in sellers
payments.order_id exists in orders
reviews.order_id exists in orders
```

## 11.3 Domain rules

```text
review_score BETWEEN 1 AND 5
price >= 0
freight_value >= 0
payment_value >= 0
payment_installments >= 0
product_weight_g >= 0
```

## 11.4 Timestamp rules

```text
purchase_timestamp is not null
approval_timestamp is after purchase timestamp
delivery timestamp is after purchase timestamp
estimated delivery date is after purchase timestamp
```

## 11.5 Reconciliation checks

At the end of each job, record:

```text
source_row_count
valid_row_count
rejected_row_count
duplicate_row_count
target_row_count
```

Expected control:

```text
source rows =
valid rows + rejected rows + removed exact duplicates
```

---

# 12. Incremental Silver loading

Although Olist is a static dataset, design the pipeline as if new files arrive daily.

## 12.1 Incremental strategy

Use:

* source file registry;
* ingestion batch ID;
* maximum processed timestamp;
* row hash;
* Iceberg `MERGE INTO`;
* idempotent job logic.

## 12.2 Upsert example

For entity tables:

```text
MERGE source records into silver table
ON business key matches

WHEN MATCHED AND source hash changed
    UPDATE record

WHEN NOT MATCHED
    INSERT record
```

Recommended business keys:

| Table       | Key                          |
| ----------- | ---------------------------- |
| Customers   | customer_id                  |
| Orders      | order_id                     |
| Products    | product_id                   |
| Sellers     | seller_id                    |
| Order items | order_id, order_item_id      |
| Payments    | order_id, payment_sequential |
| Reviews     | review_id, order_id          |

## 12.3 Idempotency

A pipeline is idempotent when rerunning the same batch does not duplicate its records.

Implement this using:

* batch-level source-file tracking;
* unique business keys;
* deduplication;
* deterministic transformations;
* Iceberg merge operations.

---

# 13. Silver physical design

Physical design should reflect expected query patterns, not merely source-file structure.

The Olist dataset is relatively small, so optimization features are primarily educational. Do not create excessive partitions or buckets that produce tiny files.

## 13.1 Partitioning concepts

Partitioning divides a table into groups based on a transform of one or more columns.

Iceberg supports hidden partitioning: users filter on the logical source column while Iceberg applies the appropriate partition transform internally.

Examples:

```text
days(order_purchase_timestamp)
months(order_purchase_timestamp)
bucket(16, customer_unique_id)
bucket(16, seller_id)
truncate(2, customer_state)
```

## 13.2 Recommended Silver partitioning

### Orders

```text
PARTITIONED BY months(order_purchase_timestamp)
```

Reason:

* orders are commonly filtered by purchase period;
* the dataset covers multiple years;
* monthly partitions avoid excessive granularity.

### Order items

Possible design:

```text
PARTITIONED BY bucket(16, order_id)
```

However, this table does not contain purchase time unless it is enriched with the order timestamp.

A better analytical Silver table may include `order_purchase_timestamp` and use:

```text
PARTITIONED BY months(order_purchase_timestamp)
```

### Payments

Enrich with order purchase timestamp and use:

```text
PARTITIONED BY months(order_purchase_timestamp)
```

### Reviews

Use:

```text
PARTITIONED BY months(review_creation_date)
```

### Customers

For this dataset:

```text
Unpartitioned
```

Customers are not large enough to justify partitioning.

At production scale, possible transforms include:

```text
bucket(32, customer_unique_id)
```

### Products

```text
Unpartitioned
```

Possible large-scale design:

```text
bucket(16, product_id)
```

### Sellers

```text
Unpartitioned
```

### Geolocation

```text
Unpartitioned
```

For a much larger table, partitioning or clustering by state and ZIP prefix could be considered.

## 13.3 Avoid high-cardinality identity partitions

Do not use:

```text
PARTITIONED BY order_id
PARTITIONED BY customer_id
PARTITIONED BY product_id
```

These values have high cardinality and would generate a very large number of tiny partitions.

## 13.4 Avoid over-partitioning

For Olist, do not partition orders by day unless you deliberately generate a much larger synthetic dataset.

Monthly partitioning is enough.

Bad design:

```text
year / month / day / state / status
```

This creates too many small partition combinations.

Better design:

```text
months(order_purchase_timestamp)
```

Then use a sort order for secondary access patterns.

---

# 14. Bucketing and clustering

## 14.1 Bucketing

Bucketing hashes a high-cardinality field into a fixed number of groups.

Iceberg supports bucket partition transforms such as:

```text
bucket(16, customer_unique_id)
bucket(16, seller_id)
bucket(32, order_id)
```

Bucketing is useful when:

* tables are large;
* joins repeatedly use the same key;
* equality filters are common;
* a fixed number of data groups is preferable to one partition per value.

For this project, use bucketing selectively.

Recommended educational example:

```text
silver.order_details
PARTITIONED BY (
    months(order_purchase_timestamp),
    bucket(16, customer_unique_id)
)
```

However, because Olist contains only around 100,000 orders, combining monthly partitioning and bucketing may produce unnecessarily small files.

For the initial implementation, prefer:

```text
Partition: months(order_purchase_timestamp)
Sort: customer_unique_id, order_id
```

Introduce bucket transforms only after increasing the dataset volume.

## 14.2 Clustering

Clustering physically places related records near one another without creating a separate directory-like partition for every value.

In an Iceberg architecture, this can be implemented through:

* table sort order;
* data-file rewrite with sort strategy;
* optional Z-order rewriting where supported;
* Spark repartitioning before writes.

Suggested sort orders:

### Orders

```text
order_purchase_timestamp
customer_id
order_status
```

### Order items

```text
order_id
product_id
seller_id
```

### Payments

```text
order_id
payment_type
```

### Reviews

```text
order_id
review_score
```

### Order details

```text
order_purchase_timestamp
customer_unique_id
product_category_name_en
```

## 14.3 Partitioning versus bucketing versus sorting

| Technique                              | Main purpose                                       |
| -------------------------------------- | -------------------------------------------------- |
| Partitioning                           | Skip broad groups of files                         |
| Bucketing                              | Distribute high-cardinality keys into fixed groups |
| Sorting                                | Improve file statistics and row locality           |
| Spark repartitioning                   | Control write parallelism                          |
| Iceberg compaction                     | Combine small files                                |
| Z-order or multidimensional clustering | Improve pruning for several filter columns         |

Do not treat these techniques as interchangeable.

---

# 15. Silver write strategy

## 15.1 Repartition before writing

Before writing a time-partitioned table:

```text
repartition by purchase month
sortWithinPartitions by common filter or join keys
```

Example concept:

```python
df.repartition("purchase_year_month") \
  .sortWithinPartitions(
      "order_purchase_timestamp",
      "customer_unique_id"
  )
```

Do not call `coalesce(1)` for production-style output.

## 15.2 Distribution mode

Iceberg requires records to be organized appropriately for partitioned writes. Iceberg documentation notes that data must be sorted or clustered by partition for writing, depending on the configured distribution behavior.

For most partitioned tables, use a hash-based distribution mode.

Conceptual table property:

```text
write.distribution-mode = hash
```

For large analytical tables with important sort order, range distribution may be evaluated.

## 15.3 Target file size

Set a reasonable target data-file size.

Production-oriented example:

```text
write.target-file-size-bytes = 134217728
```

This represents approximately 128 MB.

For the small Olist dataset, a lower target may be more practical during local testing, such as 16–64 MB.

Do not force the dataset into hundreds of tiny files merely to imitate distributed processing.

---

# 16. Silver optimization procedures

## 16.1 Small-file compaction

Repeated incremental writes can create many small files.

Run an Iceberg data-file rewrite procedure to:

* combine small files;
* rewrite files near the target size;
* improve scan performance;
* reduce metadata overhead.

Conceptual procedure:

```sql
CALL olist.system.rewrite_data_files(
    table => 'silver.orders'
);
```

Iceberg’s rewrite procedures group and rewrite data files, with work divided by partition and file-group size.

## 16.2 Sort-based rewrite

For frequently queried tables:

```sql
CALL olist.system.rewrite_data_files(
    table => 'silver.order_details',
    strategy => 'sort',
    sort_order => 'order_purchase_timestamp, customer_unique_id'
);
```

## 16.3 Manifest rewrite

Iceberg manifests help plan which data files should be scanned.

Over time, run:

```sql
CALL olist.system.rewrite_manifests(
    table => 'silver.orders'
);
```

The manifest rewrite operation reorganizes manifest metadata to improve scan planning.

## 16.4 Snapshot expiration

Every Iceberg write creates a snapshot.

Periodically expire old snapshots while retaining enough history for recovery and debugging.

Conceptual maintenance:

```sql
CALL olist.system.expire_snapshots(
    table => 'silver.orders',
    older_than => TIMESTAMP '...'
);
```

Do not expire snapshots required by active jobs or auditing.

## 16.5 Orphan-file cleanup

Use orphan-file cleanup carefully to remove data files that are no longer referenced by table metadata.

Run it only after:

* confirming the retention window;
* confirming no active writes;
* backing up important metadata;
* testing in development.

---

# 17. Spark optimization

## 17.1 Adaptive Query Execution

Enable:

```text
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

Adaptive Query Execution can coalesce small shuffle partitions and optimize query execution using runtime statistics.

## 17.2 Shuffle partitions

The default shuffle partition count may be too large for a small local dataset.

For local Olist processing, begin with a relatively small value and tune it based on available cores and observed task sizes.

Example:

```text
spark.sql.shuffle.partitions=16
```

Do not treat this as a universal setting.

For a larger synthetic Olist dataset, increase it based on:

* cluster cores;
* shuffle data size;
* target task size;
* stage duration;
* spill metrics.

## 17.3 Broadcast joins

Broadcast small dimension tables such as:

* category translation;
* products;
* sellers;
* aggregated geolocation;
* date dimension.

Avoid broadcasting large fact datasets.

## 17.4 Column pruning

Select only required columns before joins.

Bad:

```text
SELECT * from every table
```

Better:

```text
select business key and required enrichment columns
```

## 17.5 Predicate pushdown

Apply filters early, particularly:

```text
purchase date range
order status
state
product category
```

Iceberg can use partition transforms and file-level metadata for pruning.

## 17.6 Handle data skew

Potential skewed fields include:

* popular product categories;
* large sellers;
* order statuses such as delivered;
* geographic states with many orders.

Possible treatments:

* enable AQE skew handling;
* repartition on a stronger key;
* use salting only for genuinely severe skew;
* pre-aggregate before a large join;
* broadcast the smaller side.

---

# 18. Gold layer

## 18.1 Purpose of Gold

Gold represents business-ready data products.

Unlike Silver, Gold is designed for a specific analytical purpose such as:

* sales reporting;
* customer intelligence;
* seller performance;
* operational delivery monitoring;
* payment analysis;
* marketing segmentation.

Gold data should be:

* easy for analysts to understand;
* stable;
* documented;
* aggregated when appropriate;
* optimized for dashboard queries;
* based only on trusted Silver tables.

---

# 19. Gold dimensional model

Use a star-schema-inspired analytical model.

## 19.1 Main fact grain

The primary sales fact should use:

```text
One row per order item
```

This allows analysis by:

* order;
* product;
* seller;
* customer;
* date;
* category;
* geography.

## 19.2 `gold.dim_date`

Columns:

```text
date_key
full_date
day
day_name
week
month
month_name
quarter
year
year_month
is_weekend
```

## 19.3 `gold.dim_customer`

Grain:

```text
One row per customer_unique_id
```

Columns:

```text
customer_key
customer_unique_id
customer_city
customer_state
customer_region
first_purchase_date
last_purchase_date
customer_tenure_days
total_orders
customer_status
```

For the simple pipeline, this can be a Type 1 dimension.

A more advanced version can implement Slowly Changing Dimension Type 2.

## 19.4 `gold.dim_product`

Columns:

```text
product_key
product_id
category_name_pt
category_name_en
weight_g
volume_cm3
weight_band
size_band
```

## 19.5 `gold.dim_seller`

Columns:

```text
seller_key
seller_id
seller_city
seller_state
seller_region
```

## 19.6 `gold.dim_geography`

Columns:

```text
geography_key
zip_code_prefix
city
state
region
latitude
longitude
```

## 19.7 `gold.fact_order_items`

Grain:

```text
One row per order item
```

Columns:

```text
order_id
order_item_id
date_key
customer_key
product_key
seller_key
customer_geography_key
seller_geography_key
order_status
item_price
freight_amount
total_item_amount
delivery_duration_days
delivery_delay_days
review_score
is_delivered
is_cancelled
is_late_delivery
```

Measures:

```text
item_revenue
freight_revenue
total_order_item_value
delivery_days
delay_days
item_count
```

---

# 20. Gold aggregate tables

## 20.1 `gold.agg_daily_sales`

Grain:

```text
One row per date
```

Metrics:

```text
order_count
delivered_order_count
cancelled_order_count
items_sold
gross_revenue
freight_revenue
total_revenue
average_order_value
average_items_per_order
unique_customers
unique_sellers
```

## 20.2 `gold.agg_monthly_sales`

Grain:

```text
One row per year-month
```

Metrics:

```text
monthly_revenue
monthly_orders
monthly_customers
average_order_value
month_over_month_revenue_growth
cancellation_rate
delivery_success_rate
```

## 20.3 `gold.agg_category_performance`

Grain:

```text
One row per month and product category
```

Metrics:

```text
units_sold
revenue
order_count
unique_customers
average_item_price
average_freight
average_review_score
late_delivery_rate
category_revenue_share
```

## 20.4 `gold.agg_seller_performance`

Grain:

```text
One row per seller and reporting month
```

Metrics:

```text
orders_received
items_sold
gross_merchandise_value
average_item_price
average_review_score
late_delivery_rate
cancellation_rate
unique_customers
active_products
```

Possible seller score:

```text
seller_score =
    weighted revenue score
  + review score
  + delivery score
  + cancellation score
```

## 20.5 `gold.agg_delivery_performance`

Grain:

```text
Date, state, seller or category
```

Metrics:

```text
average_delivery_days
median_delivery_days
average_delay_days
on_time_delivery_rate
late_delivery_rate
average_freight_value
```

## 20.6 `gold.agg_payment_performance`

Grain:

```text
Month and payment type
```

Metrics:

```text
payment_count
paid_value
average_payment_value
average_installments
payment_type_share
multi_payment_order_count
```

## 20.7 `gold.agg_state_performance`

Grain:

```text
Month and customer state
```

Metrics:

```text
orders
customers
revenue
average_order_value
average_delivery_days
late_delivery_rate
average_review_score
```

---

# 21. Gold customer analytics

## 21.1 `gold.customer_rfm`

RFM means:

* Recency: how recently the customer purchased;
* Frequency: how many orders the customer placed;
* Monetary value: how much the customer spent.

Grain:

```text
One row per customer_unique_id
```

Columns:

```text
customer_unique_id
last_purchase_date
recency_days
frequency_orders
monetary_value
recency_score
frequency_score
monetary_score
rfm_score
customer_segment
```

Possible segments:

```text
Champions
Loyal customers
Potential loyalists
New customers
At risk
Hibernating
Lost customers
```

## 21.2 `gold.customer_360`

Combine:

* customer identity;
* location;
* first and last purchase;
* number of orders;
* total spend;
* average order value;
* preferred category;
* preferred payment type;
* average review score;
* average delivery delay;
* RFM segment.

This table becomes the main customer-marketing data product.

## 21.3 Marketing use cases

Although Olist does not contain detailed ad-click or campaign data, the Gold layer can support:

* customer segmentation;
* category affinity;
* repeat-purchase analysis;
* geographic targeting;
* high-value customer identification;
* churn-risk approximation;
* cross-sell recommendations;
* seller and category promotion strategies.

Do not call this true campaign attribution unless external campaign and traffic data is added.

---

# 22. Gold partitioning and clustering

## 22.1 Fact table

Recommended partition:

```text
months(order_purchase_timestamp)
```

Recommended sort order:

```text
order_purchase_timestamp
customer_key
product_key
seller_key
```

## 22.2 Daily aggregates

A daily aggregate will be small.

Recommended:

```text
Unpartitioned
```

or, if it becomes very large:

```text
years(full_date)
```

## 22.3 Monthly category performance

Recommended:

```text
PARTITIONED BY years(month_start_date)
```

Sort by:

```text
month_start_date
category_name_en
```

## 22.4 Seller performance

Recommended at larger scale:

```text
PARTITIONED BY months(reporting_month)
```

Sort by:

```text
seller_id
reporting_month
```

## 22.5 Customer 360 and RFM

Normally:

```text
Unpartitioned
```

At significant scale:

```text
bucket(32, customer_unique_id)
```

## 22.6 Do not copy Silver partitions automatically

Silver and Gold have different query patterns.

For example:

* Silver orders are filtered by transaction date.
* Gold customer tables are accessed by customer ID or segment.
* Gold category tables are accessed by reporting month and category.
* Gold seller scorecards are accessed by seller and period.

Choose each table’s design independently.

---

# 23. Gold refresh strategy

## 23.1 Full refresh

Initially, use full refresh for small dimensions and aggregates.

Suitable for:

```text
dim_date
dim_product
dim_seller
agg_daily_sales
agg_monthly_sales
```

## 23.2 Incremental refresh

After the basic version works, implement incremental processing.

For each new order batch:

1. determine affected purchase months;
2. update Silver records;
3. identify affected Gold partitions;
4. recompute only those periods;
5. use dynamic partition overwrite or merge;
6. validate totals.

Be careful with overwrite behavior. Iceberg documentation distinguishes static and dynamic overwrite semantics; an incorrect static overwrite can replace more data than intended.

Prefer:

* `MERGE INTO` for entity-level updates;
* dynamic overwrite for affected aggregates;
* full overwrite only for small derived tables.

---

# 24. Data-quality framework

Implement quality checks at three levels.

## 24.1 Source-level checks

```text
Expected files exist
Files are not empty
Expected columns exist
File checksum is recorded
```

## 24.2 Silver-level checks

```text
Primary keys are unique
Mandatory fields are not null
Numeric fields are valid
Timestamps are logically ordered
Foreign keys resolve
Rejected-record rate remains acceptable
```

## 24.3 Gold-level checks

```text
Revenue is not negative
Daily revenue equals detailed fact revenue
Monthly revenue equals sum of daily revenue
Order counts reconcile with Silver
Customer totals reconcile with facts
No dimension keys are unexpectedly missing
```

## 24.4 Data-quality output

Store every result in:

```text
governance.data_quality_results
```

Suggested columns:

```text
run_id
table_name
rule_name
rule_type
checked_at
rows_checked
rows_failed
failure_percentage
status
details
```

---

# 25. Pipeline orchestration

## 25.1 Recommended DAG

```text
start
  │
  ▼
check_source_files
  │
  ▼
load_bronze_tables
  │
  ▼
validate_bronze
  │
  ├── failure → stop and alert
  │
  ▼
build_silver_dimensions
  │
  ▼
build_silver_transactions
  │
  ▼
validate_silver
  │
  ├── failure → stop and alert
  │
  ▼
build_gold_dimensions
  │
  ▼
build_gold_facts
  │
  ▼
build_gold_aggregates
  │
  ▼
validate_gold
  │
  ▼
run_iceberg_maintenance
  │
  ▼
publish_pipeline_metrics
  │
  ▼
end
```

## 25.2 Job grouping

Suggested Spark jobs:

```text
01_ingest_bronze.py
02_clean_customers.py
03_clean_products.py
04_clean_sellers.py
05_clean_orders.py
06_clean_order_items.py
07_clean_payments.py
08_clean_reviews.py
09_clean_geolocation.py
10_build_order_details.py
11_build_dimensions.py
12_build_fact_order_items.py
13_build_sales_aggregates.py
14_build_customer_rfm.py
15_build_customer_360.py
16_run_quality_checks.py
17_maintain_iceberg_tables.py
```

For a simpler repository, combine related transformations into fewer jobs.

---

# 26. Repository structure

```text
olist-lakehouse/
├── docker/
│   ├── docker-compose.yml
│   ├── spark/
│   ├── minio/
│   ├── iceberg/
│   ├── trino/
│   └── airflow/
│
├── config/
│   ├── dev.yml
│   ├── test.yml
│   └── prod.yml
│
├── data/
│   └── sample/
│
├── src/
│   ├── common/
│   │   ├── spark_session.py
│   │   ├── config_loader.py
│   │   ├── quality.py
│   │   ├── audit.py
│   │   └── transformations.py
│   │
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── maintenance/
│
├── sql/
│   ├── ddl/
│   ├── quality/
│   └── analytics/
│
├── dags/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
│
├── notebooks/
├── dashboards/
├── scripts/
├── docs/
├── Makefile
├── requirements.txt
└── README.md
```

---

# 27. Testing plan

## 27.1 Unit tests

Test reusable functions:

* timestamp parsing;
* city normalization;
* category translation;
* delivery-delay calculation;
* customer-region mapping;
* review-score categorization;
* RFM scoring.

## 27.2 Dataframe tests

Use small input DataFrames and verify:

* expected columns;
* expected types;
* deduplication;
* null treatment;
* derived metrics;
* rejected rows.

## 27.3 Integration tests

Test:

* Spark connection to MinIO;
* Iceberg catalog registration;
* table creation;
* append;
* merge;
* overwrite;
* snapshot creation;
* table reading through Spark or Trino.

## 27.4 Idempotency tests

Run the same batch twice.

Expected result:

```text
No duplicate business records
No duplicated revenue
Stable row counts
No unnecessary changed records
```

## 27.5 Reconciliation tests

Examples:

```text
SUM(fact_order_items.item_price)
=
SUM(silver.order_items.price)
```

```text
SUM(agg_daily_sales.gross_revenue)
=
SUM(fact_order_items.item_price)
```

---

# 28. Observability and auditing

For every Spark job, collect:

```text
run ID
job name
start time
end time
duration
status
source row count
target row count
rejected row count
inserted row count
updated row count
snapshot ID
error message
```

Store this in:

```text
governance.pipeline_runs
```

Monitor:

* failed jobs;
* abnormal row-count changes;
* data freshness;
* rejected-record percentage;
* Spark stage duration;
* executor failures;
* shuffle spill;
* number of Iceberg files;
* average file size;
* number of snapshots.

---

# 29. Dashboard plan

## Dashboard 1 — Executive sales overview

KPIs:

```text
Total revenue
Number of orders
Average order value
Number of customers
Items sold
Cancellation rate
On-time delivery rate
Average review score
```

Charts:

* monthly revenue trend;
* orders by status;
* revenue by state;
* top product categories;
* top sellers.

## Dashboard 2 — Customer and marketing analytics

KPIs:

```text
Repeat customers
New customers
High-value customers
Average customer value
Average order frequency
```

Charts:

* RFM segment distribution;
* customer value by state;
* category affinity;
* first-time versus repeat customers;
* customer cohort retention.

## Dashboard 3 — Delivery and satisfaction

KPIs:

```text
Average delivery time
Late-delivery rate
Average delay
Average review score
```

Charts:

* delay by state;
* review score versus delay;
* seller late-delivery ranking;
* category delivery performance.

## Dashboard 4 — Seller performance

KPIs:

```text
Active sellers
Seller GMV
Average seller score
Average seller review
```

Charts:

* top sellers by revenue;
* seller delivery score;
* seller cancellation rate;
* seller revenue growth.

---

# 30. Optional dataset scaling

The original dataset is good for demonstrating modeling but relatively small for proving Spark scalability.

Create a scalable version by:

1. duplicating orders with new identifiers;
2. shifting timestamps across additional years;
3. generating synthetic customers;
4. generating new order items;
5. preserving referential integrity;
6. adding controlled data-quality errors;
7. writing incremental daily batches.

Target sizes:

```text
Level 1: Original dataset
Level 2: 1 million order items
Level 3: 10 million order items
Level 4: 100 million order items
```

This will make it possible to compare:

* unpartitioned versus partitioned tables;
* unsorted versus sorted files;
* uncompacted versus compacted layouts;
* broadcast versus shuffle joins;
* AQE disabled versus enabled;
* full refresh versus incremental refresh.

---

# 31. Recommended implementation stages

## Stage 1 — Minimum viable Lakehouse

Deliver:

* MinIO;
* Spark;
* Iceberg catalog;
* Bronze ingestion;
* five core Silver tables;
* one Gold fact;
* daily and monthly sales tables.

Core tables:

```text
silver.customers
silver.orders
silver.order_items
silver.products
silver.sellers

gold.fact_order_items
gold.agg_daily_sales
gold.agg_monthly_sales
```

## Stage 2 — Complete business model

Add:

```text
silver.payments
silver.reviews
silver.geolocation
silver.order_details

gold.dim_customer
gold.dim_product
gold.dim_seller
gold.agg_category_performance
gold.agg_seller_performance
gold.agg_delivery_performance
```

## Stage 3 — Customer and marketing intelligence

Add:

```text
gold.customer_rfm
gold.customer_360
gold.product_scorecard
gold.seller_scorecard
```

## Stage 4 — Incremental architecture

Add:

* batch registry;
* idempotent ingestion;
* Iceberg merges;
* incremental Gold refresh;
* rejected-record handling;
* pipeline auditing.

## Stage 5 — Optimization

Add:

* partition evolution;
* sort orders;
* compaction;
* snapshot expiration;
* manifest rewriting;
* Spark AQE tuning;
* performance benchmarks.

## Stage 6 — Consumption

Add:

* Trino;
* Superset or Metabase;
* four dashboards;
* SQL analytics views.

---

# 32. Suggested delivery timeline

## Week 1 — Infrastructure

* Prepare Docker Compose.
* Configure MinIO.
* Configure Spark.
* Configure Iceberg catalog.
* Verify Iceberg read and write operations.

## Week 2 — Bronze

* Download and document Olist data.
* Implement landing-zone upload.
* Define source schemas.
* Implement Bronze ingestion.
* Add ingestion metadata and audit tables.

## Week 3 — Core Silver entities

* Transform customers.
* Transform products.
* Transform sellers.
* Transform geolocation.
* Add validation and rejection handling.

## Week 4 — Silver transactions

* Transform orders.
* Transform order items.
* Transform payments.
* Transform reviews.
* Build payment and review summaries.

## Week 5 — Gold dimensional model

* Build date dimension.
* Build customer, product and seller dimensions.
* Build `fact_order_items`.
* Validate joins and metrics.

## Week 6 — Gold aggregates

* Build daily and monthly sales.
* Build category performance.
* Build seller performance.
* Build delivery and payment analytics.

## Week 7 — Customer analytics

* Build RFM segmentation.
* Build Customer 360.
* Add marketing-oriented metrics.
* Validate customer-level totals.

## Week 8 — Optimization and presentation

* Test partition strategies.
* Add sort orders.
* Run file compaction.
* Rewrite manifests.
* Configure snapshot retention.
* Run performance comparisons.
* Create dashboards.
* Document architecture and results.

---

# 33. Final simplified pipeline

```text
1. Download Olist files
2. Upload original CSV files to MinIO landing
3. Read CSV files with explicit Spark schemas
4. Add ingestion metadata
5. Write Bronze data
6. Validate source structure and row counts
7. Clean and standardize each entity
8. Deduplicate using business keys
9. Validate relationships between tables
10. Write Silver Iceberg tables
11. Partition transactional tables by purchase month
12. Sort files by important filter and join keys
13. Build dimensions and order-item fact
14. Build daily, monthly, category and seller aggregates
15. Build RFM and Customer 360
16. Write Gold Iceberg tables
17. Run reconciliation and quality tests
18. Compact small files
19. Rewrite manifests
20. Expire old snapshots according to retention policy
21. Query Gold through Spark SQL or Trino
22. Build sales, customer, delivery and seller dashboards
```

# 34. Recommended final architecture decision

For the initial implementation, use this physical design:

```text
Bronze
Raw CSV or Parquet on MinIO
No complex partitioning
Immutable source data

Silver
Apache Iceberg
Orders partitioned by purchase month
Transactional tables enriched with purchase month
Small dimensions unpartitioned
Sort transactional records by IDs and timestamps
MERGE INTO for incremental updates

Gold
Apache Iceberg
Star-schema-inspired model
Fact table partitioned by purchase month
Small dimensions unpartitioned
Aggregates partitioned only when sufficiently large
Customer tables clustered or bucketed only after scaling
```

The most important design rule is:

> Do not add partitions, buckets and optimizations merely because the technologies support them. Apply each optimization only when it matches the table volume, update pattern and query pattern.
