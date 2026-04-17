# Real-Time Retail Analytics Dashboard

> An end-to-end, Azure-native streaming pipeline that captures 5 real-time e-commerce events, processes them through a Medallion Lakehouse on Databricks, and delivers live KPI dashboards in Power BI.

---

## Table of Contents

- [Real-Time Retail Analytics Dashboard](#real-time-retail-analytics-dashboard)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
  - [Architecture](#architecture)
  - [Repository Structure](#repository-structure)
  - [Tech Stack](#tech-stack)
  - [The 5 Event Types](#the-5-event-types)
  - [Setup \& Prerequisites](#setup--prerequisites)
    - [Azure Resources Required](#azure-resources-required)
    - [Python Dependencies (local machine)](#python-dependencies-local-machine)
    - [Databricks Cluster Libraries](#databricks-cluster-libraries)
  - [Phase-by-Phase Setup Guide](#phase-by-phase-setup-guide)
    - [Phase 1 — Azure Infrastructure \& Event Generator](#phase-1--azure-infrastructure--event-generator)
    - [Phase 2 — Bronze Layer: Streaming Ingestion](#phase-2--bronze-layer-streaming-ingestion)
    - [Phase 3 — Silver Layer: Parse \& Clean](#phase-3--silver-layer-parse--clean)
    - [Phase 4 — Gold Layer: Modelling \& KPIs](#phase-4--gold-layer-modelling--kpis)
    - [Phase 5 — Power BI Dashboard](#phase-5--power-bi-dashboard)
  - [Gold KPI Tables](#gold-kpi-tables)
    - [`gold_funnel_daily`](#gold_funnel_daily)
    - [`gold_revenue_by_category`](#gold_revenue_by_category)
    - [`gold_top_products`](#gold_top_products)
    - [`gold_user_segments`](#gold_user_segments)
    - [`gold_region_performance`](#gold_region_performance)
    - [`gold_hourly_traffic`](#gold_hourly_traffic)
  - [Key Design Decisions](#key-design-decisions)
  - [Security Notes](#security-notes)
  - [Business Questions Answered](#business-questions-answered)

---

## Project Overview

This project simulates an Amazon-style e-commerce platform by generating live user behaviour events via a Python application. Events flow through the following layers:

```
Python Generator → Azure Event Hubs → Spark Structured Streaming (Databricks)
→ Delta Lake (Bronze → Silver → Gold) → Power BI Live Dashboard
```

The pipeline covers every layer of a modern data engineering stack: **ingestion**, **streaming**, **transformation**, **data modelling**, **orchestration**, and **business intelligence** — all running natively on Azure.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  INGEST                                                              │
│  Python Generator ──JSON──► Azure Event Hubs ──Kafka──► Databricks  │
│  (retail_app.py)              (retail-events hub)      Spark Stream  │
└─────────────────────────────────────────────────────────────────────┘
                                    │ Raw data
┌─────────────────────────────────────────────────────────────────────┐
│  STORE — ADLS Gen2 (Delta Lake Medallion Architecture)              │
│                                                                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│  │   BRONZE    │───►│   SILVER    │───►│    GOLD     │             │
│  │  Raw JSON   │    │ Parsed/Clean│    │ KPI tables  │             │
│  │  +metadata  │    │ Typed cols  │    │ Star schema │             │
│  └─────────────┘    └─────────────┘    └─────────────┘             │
│                                               │                      │
│                                        Unity Catalog                 │
└─────────────────────────────────────────────────────────────────────┘
                                               │ SQL Warehouse / DirectQuery
┌─────────────────────────────────────────────────────────────────────┐
│  SERVE — Power BI Dashboard                                          │
│  Funnel Analysis │ Revenue Trends │ Category Performance │ User Behaviour │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
Real-Time-Retail-Analytics-Dashboard/
│
├── source-app/
│   ├── retail_app.py              # Event schema & Faker-based generator
│   └── hub_connection.py          # Sends events to Azure Event Hubs every 2s
│
├── configs/
│   └── constant.py                # Central config (namespace, hub, storage names)
│
├── utils/
│   └── transformation_function.py # Reusable PySpark transformation helpers
│
├── bronze/
│   └── event_hub_data.ipynb       # Databricks notebook: Kafka → Bronze Delta
│
├── silver/
│   └── silver_transformations.ipynb  # Databricks notebook: Bronze → Silver Delta
│
├── gold/
│   └── Star.ipynb                 # Databricks notebook: Silver → Star schema + 6 KPI tables
│
└── README.md
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event source | Python 3, `azure-eventhub` SDK, `Faker` |
| Streaming ingestion | Azure Event Hubs (Kafka-compatible API) |
| Stream processing | Azure Databricks, Spark Structured Streaming |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows (15-min Gold batch refresh) |
| Visualisation | Power BI Desktop, Databricks SQL Warehouse, DirectQuery |
| Secrets | Azure Key Vault (recommended) |

---

## The 5 Event Types

Every event emitted by `retail_app.py` represents a step in the e-commerce user journey:

| # | Event Type | Key Fields |
|---|---|---|
| 1 | `search` | keyword, category, timestamp |
| 2 | `click` | product_id, price, category |
| 3 | `cart` | cart_id, quantity, session_id |
| 4 | `checkout` | cart_total, user_id, region |
| 5 | `purchase` | order_id, amount, payment_method |

**Common fields on every event:**
`event_id`, `user_id`, `session_id`, `event_type`, `product_id`, `category`, `timestamp`, `region`, `device_type`, `app_version`

---

## Setup & Prerequisites

### Azure Resources Required

1. **Azure Resource Group** — logical container for all resources
2. **ADLS Gen2 Storage Account** (`anilacc`) with three containers:
   - `retail-bronze`
   - `retail-silver`
   - `retail-gold`
3. **Azure Event Hubs Namespace** (`anils-namespace`) with one hub: `retail-event`
4. **Azure Databricks Workspace** with a running cluster
5. **Unity Catalog** — enabled on your Databricks workspace; catalog named `retail_catalog`, schema `gold`
6. **Azure Key Vault** — for storing all connection strings and keys securely (see [Security Notes](#security-notes))

### Python Dependencies (local machine)

```bash
pip install azure-eventhub faker
```

### Databricks Cluster Libraries

Install the following on your Databricks cluster:
- `azure-eventhub` (for SDK usage if needed)
- Delta Lake is pre-installed on Databricks Runtime 12+

---

## Phase-by-Phase Setup Guide

### Phase 1 — Azure Infrastructure & Event Generator

**1. Create Azure infrastructure**

In the Azure portal:
- Create a Resource Group
- Create an ADLS Gen2 storage account; create containers: `retail-bronze`, `retail-silver`, `retail-gold`
- Create an Event Hubs Namespace; inside it, create an Event Hub named `retail-event`

**2. Configure `configs/constant.py`**

Update this file with your own resource names. Do **not** commit real secrets to Git — move all connection strings to Azure Key Vault and retrieve them at runtime:

```python
NAMESPACE = "your-namespace"
EVENT_HUB = "your-eventhub-name"
STORAGE_ACCOUNT_NAME = "your-storage-account"
BRONZE_DATA = "retail-bronze"
SILVER_DATA = "retail-silver"
GOLD_DATA = "retail-gold"
CATALOG = "retail_catalog"
SCHEMA = "gold"
```

**3. Run the event generator**

```bash
cd source-app
python hub_connection.py
```

`hub_connection.py` calls `generate_event()` from `retail_app.py` every 2 seconds and sends the JSON payload as an `EventData` batch to Event Hubs. You should see console output like:

```
✅ SENT: {'event_id': '...', 'event_type': 'search', 'user_id': 412, ...}
✅ SENT: {'event_id': '...', 'event_type': 'purchase', 'amount': 99.5, ...}
```

**4. Verify in Azure Portal**

Open your Event Hubs Namespace → `retail-event` hub → Metrics. Confirm incoming messages are registering.

---

### Phase 2 — Bronze Layer: Streaming Ingestion

Open `bronze/event_hub_data.ipynb` in Databricks.

**How it works:**

The notebook connects to Event Hubs using the Kafka-compatible protocol over SASL_SSL. The `ehConf` dictionary configures the Kafka bootstrap server, authentication, and the hub to subscribe to.

```python
ehConf = {
  "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
  "kafka.security.protocol": "SASL_SSL",
  "kafka.sasl.mechanism": "PLAIN",
  "subscribe": f"{EVENT_HUB}",
  "startingOffsets": "latest",
  "kafka.group.id": "databricks-consumer"
}

df = spark.readStream.format("kafka").options(**ehConf).load()
```

The raw Kafka `value` bytes are cast to string, and three metadata columns are added:

```python
df_bronze = df.selectExpr("CAST(value AS STRING)", "partition") \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("event_hub_partition", col("partition")) \
    .withColumn("source", lit("event_hub"))
```

The stream is written to the Bronze Delta table with checkpointing for fault tolerance:

```python
df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://retail-bronze@<account>.dfs.core.windows.net/checkpoints/kafka_bronze/") \
    .trigger(availableNow=True) \
    .start("abfss://retail-bronze@<account>.dfs.core.windows.net/kafka_data/")
```

> **Design decision:** `trigger(availableNow=True)` processes all available data at startup and stops — suitable for scheduled runs. Switch to `trigger(processingTime='10 seconds')` for always-on streaming.

---

### Phase 3 — Silver Layer: Parse & Clean

Open `silver/silver_transformations.ipynb` in Databricks.

**How it works:**

The Silver layer reads from the Bronze Delta table as a streaming source, then applies a defined schema to parse the raw JSON string:

```python
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("payment_method", StringType()),
    # ... all fields
])

df_parsed = df_bronze \
    .withColumn("decoded", col("value").cast("string")) \
    .withColumn("json", from_json(col("decoded"), schema)) \
    .select("json.*", "decoded", "ingestion_timestamp")
```

Four reusable helper functions from `utils/transformation_function.py` are applied in sequence:

| Function | What it does |
|---|---|
| `Add_newField(df)` | Extracts any new/optional field from the JSON using `get_json_object` — future-proofs the schema |
| `Parse_Time(df)` | Casts `timestamp` to `TimestampType`; derives `event_date`, `event_hour`, `event_week` |
| `Filtering(df)` | Drops rows with null `user_id`; drops unknown `event_type` values |
| `Remove_Duplicate(df)` | Calls `dropDuplicates(["event_id"])` to ensure idempotency |

The cleaned data is written to Silver, partitioned by `event_date` and `event_type`:

```python
df_final.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://retail-silver@<account>.dfs.core.windows.net/checkpoints/") \
    .partitionBy("event_date", "event_type") \
    .start("abfss://retail-silver@<account>.dfs.core.windows.net/clean_data/")
```

---

### Phase 4 — Gold Layer: Modelling & KPIs

Open `gold/Star.ipynb` in Databricks.

**How it works:**

Gold reads from Silver as a **batch** (not streaming), applies incremental logic to avoid reprocessing old data, then builds a full star schema.

**Incremental load pattern:**

```python
try:
    last_processed = spark.table("retail_catalog.gold.fct_events") \
        .agg(max("event_timestamp")).collect()[0][0]
except:
    last_processed = None

df = df_silver.filter(col("event_timestamp") > last_processed) if last_processed else df_silver
```

**Dimension tables built:**

| Table | Key | Columns |
|---|---|---|
| `dim_product` | `product_key` | `product_id`, `category` |
| `dim_user` | `user_key` | `user_id`, `region`, `device_type` |
| `dim_region` | `region_key` | `region` |
| `dim_date` | `date_key` | `event_date`, `event_hour`, `week` |

Surrogate keys are generated using `dense_rank()` over a Window ordered by the natural key.

**Fact table `fct_events`:**

All four dimension tables are broadcast-joined to the Silver events to produce the central fact table:

```python
fct_events = f \
    .join(broadcast(p), col("f.product_id") == col("p.product_id"), "left") \
    .join(broadcast(u), col("f.user_id") == col("u.user_id"), "left") \
    .join(broadcast(d), col("f.event_date") == col("d.event_date"), "left") \
    .join(broadcast(r), col("f.region") == col("r.region"), "left") \
    .select(surrogate_keys + fact_columns) \
    .dropDuplicates(["event_id"])

fct_events.write.format("delta").mode("append").saveAsTable("retail_catalog.gold.fct_events")
```

All 6 KPI tables are then derived from `fct_events` using Spark SQL (see [Gold KPI Tables](#gold-kpi-tables) below).

**Schedule:** Run `gold/Star.ipynb` as a Databricks Workflow job every 15 minutes for near-real-time Gold refresh.

---

### Phase 5 — Power BI Dashboard

1. Open **Power BI Desktop**
2. Go to **Get Data → Azure → Azure Databricks**
3. Use **Partner Connect** in your Databricks workspace to generate the SQL Warehouse connection URL
4. Connect using **DirectQuery** mode for near-real-time data (or configure scheduled refresh)
5. Connect to the following Gold tables in `retail_catalog.gold`:
   - `gold_funnel_daily`
   - `gold_revenue_by_category`
   - `gold_top_products`
   - `gold_user_segments`
   - `gold_region_performance`
   - `gold_hourly_traffic`

**DAX measure for conversion rate:**

```dax
Conversion Rate =
DIVIDE(
    CALCULATE(COUNTROWS(gold_funnel_daily), gold_funnel_daily[purchases]),
    CALCULATE(COUNTROWS(gold_funnel_daily), gold_funnel_daily[searches])
)
```

**Add slicers for:** date range, category, region, device type.

**4 Report Pages:**

| Page | Visuals |
|---|---|
| Funnel Analysis | Funnel chart from `gold_funnel_daily`; drop-off % between stages |
| Revenue Trends | Line chart of weekly revenue by category from `gold_revenue_by_category` |
| Category Performance | Bar chart of top categories by revenue and order count |
| User Behaviour | Heatmap of hourly traffic from `gold_hourly_traffic`; region map from `gold_region_performance` |

---

## Gold KPI Tables

All 6 tables live in `retail_catalog.gold` and are refreshed every 15 minutes via Databricks Workflows.

### `gold_funnel_daily`
Count of each event type per day — answers *"Where do users drop off?"*

```sql
SELECT event_date,
  COUNT(CASE WHEN event_type='search'   THEN 1 END) AS searches,
  COUNT(CASE WHEN event_type='click'    THEN 1 END) AS clicks,
  COUNT(CASE WHEN event_type='cart'     THEN 1 END) AS carts,
  COUNT(CASE WHEN event_type='checkout' THEN 1 END) AS checkouts,
  COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases
FROM retail_catalog.gold.fct_events
GROUP BY event_date
```

### `gold_revenue_by_category`
Revenue, order count, and AOV by category and date — powers the revenue trend chart.

```sql
SELECT category, event_date,
  SUM(amount) AS total_revenue,
  COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS total_orders,
  AVG(amount) AS avg_order_value
FROM retail_catalog.gold.fct_events
WHERE event_type = 'purchase'
GROUP BY category, event_date
```

### `gold_top_products`
Clicks, cart adds, and purchases per product — reveals which products convert vs. just attract views.

```sql
SELECT product_id,
  COUNT(CASE WHEN event_type='click'    THEN 1 END) AS clicks,
  COUNT(CASE WHEN event_type='cart'     THEN 1 END) AS carts,
  COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases
FROM retail_catalog.gold.fct_events
GROUP BY product_id
```

### `gold_user_segments`
Active days, total events, and purchase count per user — segments users into browsers, abandoners, and buyers.

```sql
SELECT user_id,
  COUNT(DISTINCT event_date)                        AS active_days,
  COUNT(*)                                          AS total_events,
  SUM(CASE WHEN event_type='purchase' THEN 1 END)  AS purchases
FROM retail_catalog.gold.fct_events
GROUP BY user_id
```

### `gold_region_performance`
Revenue and conversion rate by region — identifies best-performing geographies.

```sql
SELECT region_key,
  COUNT(*) AS total_events,
  SUM(CASE WHEN event_type='purchase' THEN amount ELSE 0 END) AS revenue,
  COUNT(CASE WHEN event_type='purchase' THEN 1 END) * 1.0 /
  COUNT(CASE WHEN event_type='search'   THEN 1 END) AS conversion_rate
FROM retail_catalog.gold.fct_events
GROUP BY region_key
```

### `gold_hourly_traffic`
Event count and revenue by hour of day — used for peak traffic analysis and capacity planning.

```sql
SELECT event_hour,
  COUNT(*) AS total_events,
  SUM(CASE WHEN event_type='purchase' THEN amount ELSE 0 END) AS revenue
FROM retail_catalog.gold.fct_events
GROUP BY event_hour
```

---

## Key Design Decisions

**1. Kafka protocol over native Event Hubs SDK in Spark**
Databricks' Kafka connector is more mature and widely supported than the native Event Hubs connector. Using the Kafka-compatible endpoint of Event Hubs (port 9093, SASL_SSL) gives access to offset management, consumer groups, and `failOnDataLoss` control — all of which are critical for reliable streaming.

**2. Medallion Architecture (Bronze → Silver → Gold)**
Each layer has a clear contract:
- Bronze preserves raw data exactly as received — never mutated, always replayable.
- Silver is the canonical clean dataset — typed, deduplicated, partitioned.
- Gold is business-facing — aggregated, modelled, and optimised for Power BI queries.
This separation means bugs in Silver or Gold transformations can be fixed by replaying from Bronze or Silver without re-ingesting from Event Hubs.

**3. Reusable transformation functions in `utils/`**
Instead of duplicating logic across notebooks, all Silver transformations (`Add_newField`, `Parse_Time`, `Filtering`, `Remove_Duplicate`) live in `utils/transformation_function.py`. This makes them independently testable and reusable if a new Silver table is needed in future.

**4. Schema-on-read with `from_json()` and a defined `StructType`**
Defining the schema explicitly (rather than using schema inference) avoids the cost of scanning the entire dataset and provides a stable contract. The `Add_newField` helper provides a safe extension point when new fields are introduced by the generator.

**5. Broadcast joins in the Gold star schema**
Dimension tables (`dim_product`, `dim_user`, `dim_date`, `dim_region`) are small relative to the fact table. Broadcasting them to all executors eliminates shuffle during the join — critical at scale when `fct_events` is large.

**6. Incremental Gold load**
The Gold notebook checks `max(event_timestamp)` in `fct_events` before reading Silver, and filters Silver to only new events. This avoids recomputing all historical KPIs on every 15-minute run.

**7. Partitioning strategy**
Silver is partitioned by `event_date` and `event_type` — the two most common filter predicates in Power BI queries. This reduces scan cost significantly when Power BI requests data for a specific date range or event type.

---

## Security Notes

> ⚠️ The current `configs/constant.py` and `source-app/hub_connection.py` contain hardcoded connection strings and access keys. **This is not safe for production or any shared repository.**

**Recommended fix — move all secrets to Azure Key Vault:**

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://<your-vault>.vault.azure.net/", credential=credential)

CONNECTION_STRING = client.get_secret("eventhub-connection-string").value
STORAGE_ACCOUNT_ACCESS_KEY = client.get_secret("adls-access-key").value
```

In Databricks notebooks, use **Databricks Secrets** (`dbutils.secrets.get`) instead:

```python
connection_string = dbutils.secrets.get(scope="retail-scope", key="eventhub-connection-string")
```

Add `configs/constant.py` to `.gitignore` to prevent accidental commits of credentials.

---

## Business Questions Answered

| Business Question | Source Table | Power BI Page |
|---|---|---|
| Where do users drop off between search and purchase? | `gold_funnel_daily` | Funnel Analysis |
| Which categories drive the most revenue this week vs last? | `gold_revenue_by_category` | Revenue Trends |
| What % of searches result in a purchase, by region? | `gold_region_performance` | Category Performance |
| What hours see peak traffic and peak revenue? | `gold_hourly_traffic` | User Behaviour |
| Which products convert well vs just attract clicks? | `gold_top_products` | Category Performance |
| How do users segment into browsers, abandoners, and buyers? | `gold_user_segments` | User Behaviour |

---
