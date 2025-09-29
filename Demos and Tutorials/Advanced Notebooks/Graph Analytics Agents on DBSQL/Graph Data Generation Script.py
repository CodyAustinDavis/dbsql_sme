# Databricks notebook source
# MAGIC %md
# MAGIC # STIX Data Generation Script

# COMMAND ----------

# MAGIC %pip install stix2 faker

# COMMAND ----------

TARGET_CATALOG = "main"
TARGET_SCHEMA = "threat_graph_analytics"

# COMMAND ----------

# DBTITLE 1,Define environment

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"DROP SCHEMA IF EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA} CASCADE")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

import uuid
import random
from datetime import datetime
from faker import Faker
from stix2 import ThreatActor, Malware, Indicator, Relationship
from pyspark.sql import SparkSession, Row

fake = Faker()


# COMMAND ----------

import uuid
import random
import json
from datetime import datetime
from faker import Faker
from stix2 import (
    ThreatActor, Malware, Indicator, Relationship, ObservedData,
    Identity, Tool, Infrastructure, Report, CourseOfAction
)
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, MapType

from pyspark.sql.functions import from_json, col, parse_json

# === Configuration ===
NUM_THREAT_ACTORS = 100
NUM_MALWARE = 100
NUM_CAMPAIGNS = 10
NUM_OBSERVED = 1000
NUM_TOOLS = 10
NUM_INFRASTRUCTURE = 10
NUM_REPORTS = 10
NUM_COURSE_OF_ACTION = 5
NUM_IDENTITIES = 10000

fake = Faker()

# === Reusable Core Entities ===
threat_actors = [
    ThreatActor(
        id=f"threat-actor--{uuid.uuid4()}",
        name=fake.company(),
        description=fake.text(),
        roles=["intrusion-set"],
        labels=["apt"]
    ) for _ in range(NUM_THREAT_ACTORS)
]

malware_pool = [
    Malware(
        id=f"malware--{uuid.uuid4()}",
        name=fake.word(),
        is_family=True,
        labels=["trojan", "spyware"]
    ) for _ in range(NUM_MALWARE)
]

tools = [
    Tool(
        id=f"tool--{uuid.uuid4()}",
        name=fake.word(),
        labels=["remote-access", "loader"]
    ) for _ in range(NUM_TOOLS)
]

infrastructure_list = [
    Infrastructure(
        id=f"infrastructure--{uuid.uuid4()}",
        name=fake.domain_name(),
        infrastructure_types=["command-and-control"]
    ) for _ in range(NUM_INFRASTRUCTURE)
]

identities = [
    Identity(
        id=f"identity--{uuid.uuid4()}",
        name=fake.company(),
        identity_class="organization",
        sectors=["technology", "financial-services"],
        contact_information=fake.email()
    ) for _ in range(NUM_IDENTITIES)
]

# === Generate STIX Objects ===
stix_objects = []

# Campaigns
for _ in range(NUM_CAMPAIGNS):
    ta = random.choice(threat_actors)
    malware_used = random.sample(malware_pool, k=random.randint(1, 3))
    indicators = [
        Indicator(
            id=f"indicator--{uuid.uuid4()}",
            name="Malicious IP",
            pattern_type="stix",
            pattern=f"[ipv4-addr:value = '{fake.ipv4()}']"
        ) for _ in range(random.randint(1, 2))
    ]
    relationships = []
    for mal in malware_used:
        relationships.append(Relationship(
            id=f"relationship--{uuid.uuid4()}",
            relationship_type="uses",
            source_ref=ta.id,
            target_ref=mal.id,
        ))
        for ind in indicators:
            relationships.append(Relationship(
                id=f"relationship--{uuid.uuid4()}",
                relationship_type="indicates",
                source_ref=ind.id,
                target_ref=mal.id,
            ))

    stix_objects.append(ta)
    stix_objects.extend(malware_used)
    stix_objects.extend(indicators)
    stix_objects.extend(relationships)

# Observed Data (e.g. logs, detections)
for _ in range(NUM_OBSERVED):
    first = fake.date_time_this_year()
    last = fake.date_time_between_dates(datetime_start=first)

    observed = ObservedData(
        id=f"observed-data--{uuid.uuid4()}",
        first_observed=first,
        last_observed=last,
        number_observed=random.randint(1, 10),
        objects={
            "0": {"type": "ipv4-addr", "value": fake.ipv4()},
            "1": {"type": "domain-name", "value": fake.domain_name()}
        }
    )
    stix_objects.append(observed)

# Contextual entities
stix_objects.extend(tools)
stix_objects.extend(infrastructure_list)
stix_objects.extend(identities)

# Reports
for _ in range(NUM_REPORTS):
    published = fake.date_time_this_year()
    obj_refs = random.sample([o.id for o in stix_objects if hasattr(o, "id")], k=3)
    report = Report(
        id=f"report--{uuid.uuid4()}",
        name=fake.sentence(),
        published=fake.date_time_this_year(),
        object_refs=obj_refs
    )
    stix_objects.append(report)

# Courses of Action
for _ in range(NUM_COURSE_OF_ACTION):
    coa = CourseOfAction(
        id=f"course-of-action--{uuid.uuid4()}",
        name="Apply security patch",
        description=fake.sentence()
    )
    stix_objects.append(coa)

# === Convert to Spark Rows ===
def to_row(obj):
    parsed = json.loads(obj.serialize())
    return Row(
        stix_id=parsed.get("id"),
        stix_type=parsed.get("type"),
        name=parsed.get("name", None),
        description=parsed.get("description", None),
        raw=json.dumps(parsed)  # No extra whitespace, no escape issues
    )

rows = [to_row(o) for o in stix_objects]

# === Define schema with raw as string ===
schema = StructType([
    StructField("stix_id", StringType(), True),
    StructField("stix_type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("raw", StringType(), True)
])

# === Create Spark DataFrame and Parse JSON ===
df = spark.createDataFrame(rows, schema=schema)
df = df.withColumn("raw", parse_json(col("raw")))

(df.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.full_synthetic_stix_graph")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT raw FROM main.threat_graph_analytics.full_synthetic_stix_graph

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.stix_graph_edges AS
SELECT
  raw:source_ref::string AS src,
  raw:target_ref::string AS dst,
  raw:relationship_type::string AS relationship_type,
  raw
FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.full_synthetic_stix_graph
WHERE raw:type::string = 'relationship'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.stix_graph_nodes AS
SELECT
  raw:id::string AS id,
  raw:type::string AS type,
  raw
FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.full_synthetic_stix_graph
WHERE raw:type::string != 'relationship'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.threat_graph_analytics.threat_graph_node_annotations (
# MAGIC   id STRING PRIMARY KEY,
# MAGIC   annotation STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw FROM main.threat_graph_analytics.graph_search_results