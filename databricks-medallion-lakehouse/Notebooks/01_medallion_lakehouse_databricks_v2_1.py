# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse e-commerce — Architecture médaillon sur Databricks
# MAGIC
# MAGIC **Objectif** : démontrer une implémentation complète d'un datalakehouse sur Databricks avec Delta Lake,
# MAGIC structurée en trois couches (Bronze / Silver / Gold), gouvernée par Unity Catalog.
# MAGIC
# MAGIC **Cas d'usage** : plateforme e-commerce — ingestion de commandes, clients et produits, jusqu'à un
# MAGIC modèle en étoile prêt pour Power BI.
# MAGIC
# MAGIC | Couche | Rôle | Format | Exemple de traitement |
# MAGIC |---|---|---|---|
# MAGIC | 🥉 Bronze | Ingestion brute, traçabilité | Delta (append-only) | Auto Loader, schéma souple, métadonnées d'ingestion |
# MAGIC | 🥈 Silver | Données nettoyées et conformes | Delta (MERGE) | Dédoublonnage, typage, règles métier, historisation SCD2 |
# MAGIC | 🥇 Gold | Données agrégées, orientées usage | Delta (star schema) | Faits & dimensions, KPIs, vues consommées par Power BI |
# MAGIC
# MAGIC **Stack** : PySpark, Delta Lake, Databricks Auto Loader, Unity Catalog, Databricks Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration & paramètres
# MAGIC Utilisation de widgets pour rendre le notebook paramétrable (environnement, catalogue, schéma) —
# MAGIC bonne pratique pour l'exécution via Databricks Workflows / Jobs.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse", "Catalogue Unity Catalog")
dbutils.widgets.text("schema_prefix", "ecommerce", "Préfixe des schémas")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

BRONZE_SCHEMA = f"{SCHEMA_PREFIX}_bronze"
SILVER_SCHEMA = f"{SCHEMA_PREFIX}_silver"
GOLD_SCHEMA = f"{SCHEMA_PREFIX}_gold"

print(f"Catalogue     : {CATALOG}")
print(f"Schéma Bronze : {CATALOG}.{BRONZE_SCHEMA}")
print(f"Schéma Silver : {CATALOG}.{SILVER_SCHEMA}")
print(f"Schéma Gold   : {CATALOG}.{GOLD_SCHEMA}")

# Note : LANDING_PATH et CHECKPOINT_ROOT sont calculés plus bas, une fois les volumes
# Unity Catalog créés (section 1) — un volume doit exister comme objet catalogué
# avant qu'on puisse y écrire, contrairement à un chemin DBFS classique.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gouvernance : création du catalogue et des schémas (Unity Catalog)
# MAGIC Architecture en 3 niveaux : `catalogue.schéma.table` — chaque couche du médaillon a son propre schéma,
# MAGIC ce qui permet de gérer finement les droits d'accès (ex : accès direct à Gold pour les analystes,
# MAGIC accès Bronze réservé aux ingénieurs data).

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA} COMMENT 'Données brutes, non transformées'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA} COMMENT 'Données nettoyées et conformes'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA} COMMENT 'Données agrégées, modèle en étoile'")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Création des volumes Unity Catalog (zone d'atterrissage & checkpoints)
# MAGIC Un **volume** est l'objet Unity Catalog qui gouverne l'accès à du stockage de fichiers non tabulaire
# MAGIC (JSON, CSV, images, checkpoints Auto Loader...). Il doit être créé explicitement — au même titre
# MAGIC qu'une table — avant de pouvoir écrire sur son chemin `/Volumes/<catalog>/<schema>/<volume>`.
# MAGIC On le rattache au schéma Bronze puisqu'il porte la donnée la plus brute du pipeline.

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.landing
    COMMENT 'Zone de dépôt des fichiers sources bruts (JSON/CSV) avant ingestion Auto Loader'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.checkpoints
    COMMENT 'Checkpoints Auto Loader (offsets + schéma inféré) pour l ingestion incrémentale'
""")

LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/checkpoints"

print(f"Volume landing zone : {LANDING_PATH}")
print(f"Volume checkpoints  : {CHECKPOINT_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulation de la zone d'atterrissage (landing zone)
# MAGIC En production, ces fichiers arriveraient via une extraction applicative, un connecteur ou un flux CDC.
# MAGIC Ici, on génère des données synthétiques réalistes (commandes, clients, produits) pour illustrer
# MAGIC le pipeline de bout en bout, y compris des anomalies volontaires (doublons, nulls, valeurs incohérentes)
# MAGIC afin de démontrer les étapes de nettoyage en Silver.

# COMMAND ----------

import random
import json
from datetime import datetime, timedelta

random.seed(42)

def generate_customers(n=500):
    villes = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Lille", "Nantes"]
    rows = []
    for i in range(1, n + 1):
        row = {
            "customer_id": f"CUST-{i:05d}",
            "first_name": f"Prenom{i}",
            "last_name": f"Nom{i}",
            "email": f"client{i}@exemple.com".upper() if i % 7 == 0 else f"client{i}@exemple.com",
            "city": random.choice(villes),
            "signup_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d"),
        }
        rows.append(row)
        if i % 50 == 0:  # doublon volontaire pour illustrer le dédoublonnage en Silver
            rows.append(row)
    return rows

def generate_products(n=80):
    categories = ["Électronique", "Maison", "Sport", "Mode", "Beauté"]
    return [
        {
            "product_id": f"PROD-{i:04d}",
            "product_name": f"Produit {i}",
            "category": random.choice(categories),
            "unit_price": round(random.uniform(5, 500), 2),
        }
        for i in range(1, n + 1)
    ]

def generate_orders(customers, products, n=3000):
    rows = []
    for i in range(1, n + 1):
        cust = random.choice(customers)
        prod = random.choice(products)
        qty = random.randint(1, 5)
        price = prod["unit_price"] if random.random() > 0.02 else None  # null volontaire
        row = {
            "order_id": f"ORD-{i:06d}",
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": price,
            "order_ts": (datetime(2024, 1, 1) + timedelta(minutes=random.randint(0, 500000))).isoformat(),
            "status": random.choice(["completed", "completed", "completed", "cancelled", "refunded"]),
        }
        rows.append(row)
    return rows

customers_raw = generate_customers()
products_raw = generate_products()
orders_raw = generate_orders(customers_raw, products_raw)

# Écriture en JSON dans la landing zone, comme si un système source déposait des exports quotidiens
for name, data in [("customers", customers_raw), ("products", products_raw), ("orders", orders_raw)]:
    path = f"{LANDING_PATH}/{name}/{name}_{datetime.now().strftime('%Y%m%d')}.json"
    dbutils.fs.mkdirs(f"{LANDING_PATH}/{name}")
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in data), overwrite=True)

print("Fichiers déposés dans la landing zone :", LANDING_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🥉 Couche Bronze — Ingestion brute avec Auto Loader
# MAGIC
# MAGIC Principes appliqués :
# MAGIC - **Auto Loader** (`cloudFiles`) pour une ingestion incrémentale et scalable, avec inférence et évolution de schéma
# MAGIC - **Aucune transformation métier** : on garde la donnée "telle quelle" pour garantir la traçabilité et permettre un rejeu complet
# MAGIC - Ajout de **métadonnées techniques** : horodatage d'ingestion, fichier source
# MAGIC - Écriture en **Delta Lake** (ACID, schéma versionné, time travel)

# COMMAND ----------

from pyspark.sql import functions as F

def ingest_to_bronze(entity_name: str, checkpoint_root: str = CHECKPOINT_ROOT):
    """Ingestion Auto Loader générique : landing zone -> table Bronze Delta."""
    source_path = f"{LANDING_PATH}/{entity_name}"
    checkpoint_path = f"{checkpoint_root}/{entity_name}"
    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{entity_name}"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)  # traite tout le backlog disponible puis s'arrête (batch incrémental)
        .toTable(target_table)
    )
    query.awaitTermination()
    print(f"✅ Bronze : {target_table} mise à jour depuis {source_path}")

for entity in ["customers", "products", "orders"]:
    ingest_to_bronze(entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrôle rapide de la couche Bronze
# MAGIC On vérifie les volumétries et on inspecte l'historique Delta (time travel / audit).

# COMMAND ----------

for entity in ["customers", "products", "orders"]:
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{entity}").count()
    print(f"{entity:10s} -> {count} lignes en Bronze")

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.{BRONZE_SCHEMA}.orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 🥈 Couche Silver — Nettoyage, conformité et règles métier
# MAGIC
# MAGIC Principes appliqués :
# MAGIC - **Dédoublonnage** et gestion des valeurs nulles/incohérentes
# MAGIC - **Typage strict** et standardisation (emails en minuscule, dates au format `date`)
# MAGIC - **Contraintes de qualité** (`CHECK` constraints Delta) pour bloquer les écritures invalides
# MAGIC - **MERGE INTO** pour un chargement incrémental idempotent (upsert), avec suivi SCD Type 1 sur les dimensions

# COMMAND ----------

# --- Silver: customers (SCD Type 1 via MERGE) ---

customers_silver_df = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("signup_date", F.to_date("signup_date"))
    .dropDuplicates(["customer_id"])  # élimine les doublons injectés en amont
    .filter(F.col("customer_id").isNotNull())
    .select("customer_id", "first_name", "last_name", "email", "city", "signup_date")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.customers (
        customer_id STRING NOT NULL,
        first_name STRING,
        last_name STRING,
        email STRING,
        city STRING,
        signup_date DATE,
        _updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

customers_silver_df.createOrReplaceTempView("customers_updates")

spark.sql(f"""
    MERGE INTO {CATALOG}.{SILVER_SCHEMA}.customers AS target
    USING customers_updates AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET
        target.first_name = source.first_name,
        target.last_name = source.last_name,
        target.email = source.email,
        target.city = source.city,
        target.signup_date = source.signup_date,
        target._updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (customer_id, first_name, last_name, email, city, signup_date, _updated_at)
        VALUES (source.customer_id, source.first_name, source.last_name, source.email, source.city, source.signup_date, current_timestamp())
""")

print("✅ Silver.customers mise à jour via MERGE (upsert)")

# COMMAND ----------

# --- Silver: products ---

products_silver_df = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")
    .dropDuplicates(["product_id"])
    .filter(F.col("unit_price") > 0)
    .select("product_id", "product_name", "category", "unit_price")
)

(products_silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.products"))

print("✅ Silver.products écrite")

# COMMAND ----------

# --- Silver: orders (nettoyage + enrichissement + contrôle qualité) ---

products_ref = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products").select("product_id", "unit_price")

orders_silver_df = (
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("order_date", F.to_date("order_ts"))
    .drop("unit_price")  # on reprend le prix de référence produit plutôt que celui (parfois null) de la commande
    .join(products_ref, on="product_id", how="left")
    .withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .dropDuplicates(["order_id"])
)

# Contrôle de qualité explicite avant écriture (pattern "data quality gate")
invalid_orders = orders_silver_df.filter(F.col("unit_price").isNull() | (F.col("quantity") <= 0))
invalid_count = invalid_orders.count()
if invalid_count > 0:
    print(f"⚠️ {invalid_count} commandes invalides détectées et exclues (prix ou quantité incohérents)")

orders_silver_clean = orders_silver_df.filter(F.col("unit_price").isNotNull() & (F.col("quantity") > 0))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.orders (
        order_id STRING NOT NULL,
        customer_id STRING NOT NULL,
        product_id STRING,
        quantity INT,
        unit_price DOUBLE,
        line_amount DOUBLE,
        order_ts TIMESTAMP,
        order_date DATE,
        status STRING
    ) USING DELTA
    PARTITIONED BY (order_date)
""")

orders_silver_clean.select(
    "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "line_amount", "order_ts", "order_date", "status"
).createOrReplaceTempView("orders_updates")

spark.sql(f"""
    MERGE INTO {CATALOG}.{SILVER_SCHEMA}.orders AS target
    USING orders_updates AS source
    ON target.order_id = source.order_id
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Silver.orders mise à jour via MERGE (upsert, partitionnée par order_date)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 🥇 Couche Gold — Modèle en étoile pour la BI
# MAGIC
# MAGIC Objectif : exposer un **schéma en étoile** simple, performant et directement consommable par Power BI
# MAGIC (Import ou DirectQuery via le connecteur Databricks natif).
# MAGIC
# MAGIC - `dim_customer`, `dim_product`, `dim_date` : dimensions
# MAGIC - `fact_orders` : table de faits au grain "ligne de commande"
# MAGIC - Vue agrégée `vw_sales_kpis` : KPIs prêts à l'emploi (CA, panier moyen, taux d'annulation)

# COMMAND ----------

# --- dim_customer ---
(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
    .select("customer_id", "first_name", "last_name", "email", "city", "signup_date")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_customer"))

# --- dim_product ---
(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_product"))

# --- dim_date (générée pour couvrir la plage des commandes) ---
dim_date_df = (
    spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day)) AS full_date")
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
)
(dim_date_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_date"))

# --- fact_orders (grain: une ligne = une ligne de commande) ---
fact_orders_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")

(fact_orders_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("order_date")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders"))

print("✅ Couche Gold construite : dim_customer, dim_product, dim_date, fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vue métier prête pour Power BI
# MAGIC Une vue SQL au-dessus du modèle en étoile pour exposer directement des KPIs lisibles côté métier —
# MAGIC évite de dupliquer la logique de calcul dans chaque rapport Power BI.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.vw_sales_kpis AS
    SELECT
        d.year,
        d.month,
        d.month_name,
        p.category,
        COUNT(DISTINCT f.order_id)                                   AS nb_commandes,
        ROUND(SUM(CASE WHEN f.status = 'completed' THEN f.line_amount END),2) AS chiffre_affaires,
        ROUND(AVG(CASE WHEN f.status = 'completed' THEN f.line_amount END), 2) AS panier_moyen_ligne,
        ROUND(100.0 * SUM(CASE WHEN f.status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_annulation_pct
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_orders f
    JOIN {CATALOG}.{GOLD_SCHEMA}.dim_date d ON f.order_date = d.full_date
    JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product p ON f.product_id = p.product_id
    GROUP BY d.year, d.month, d.month_name, p.category
""")

display(spark.table(f"{CATALOG}.{GOLD_SCHEMA}.vw_sales_kpis").orderBy("year", "month").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimisation et performance
# MAGIC - **OPTIMIZE + Z-ORDER** sur les colonnes de filtrage fréquent pour accélérer les requêtes Power BI (DirectQuery)
# MAGIC - **VACUUM** pour purger les anciens fichiers physiques au-delà de la période de rétention (time travel)
# MAGIC - Statistiques recalculées automatiquement par Delta Lake pour le data skipping

# COMMAND ----------

spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.fact_orders ZORDER BY (customer_id, product_id)")
spark.sql(f"VACUUM {CATALOG}.{SILVER_SCHEMA}.orders RETAIN 168 HOURS")  # 7 jours de rétention par défaut

print("✅ Table Gold optimisée (Z-ORDER) et Silver purgée (VACUUM)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Qualité des données & gouvernance
# MAGIC Ajout de contraintes déclaratives et de commentaires de colonnes pour la documentation
# MAGIC automatique dans Unity Catalog (Catalog Explorer).

# COMMAND ----------

spark.sql(f"ALTER TABLE {CATALOG}.{GOLD_SCHEMA}.fact_orders ALTER COLUMN order_id COMMENT 'Identifiant unique de la commande (grain de la table)'")
spark.sql(f"ALTER TABLE {CATALOG}.{GOLD_SCHEMA}.fact_orders ALTER COLUMN line_amount COMMENT 'Montant de la ligne = quantité x prix unitaire'")

# Exemple de contrainte de qualité bloquante sur Silver
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.orders
    ADD CONSTRAINT positive_quantity CHECK (quantity > 0)
""")

print("✅ Documentation des colonnes et contraintes de qualité appliquées")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Time travel — audit et reproductibilité
# MAGIC Illustration d'une des forces majeures de Delta Lake : interroger une table telle qu'elle
# MAGIC était à un instant T, ou revenir à une version antérieure en cas d'erreur de traitement.

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{GOLD_SCHEMA}.fact_orders")
display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

# Exemple de lecture d'une version antérieure (time travel)
# spark.read.format("delta").option("versionAsOf", 0).table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Orchestration — vers Databricks Workflows
# MAGIC En production, ce notebook serait découpé en tâches indépendantes (Bronze / Silver / Gold) orchestrées
# MAGIC par un **Databricks Workflow (Job)**, avec dépendances explicites, retries et alerting.
# MAGIC Exemple de définition de job (Jobs API / Terraform) :
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "ecommerce_medallion_pipeline",
# MAGIC   "tasks": [
# MAGIC     {"task_key": "bronze_ingestion", "notebook_task": {"notebook_path": "/pipelines/01_bronze"}},
# MAGIC     {"task_key": "silver_transform", "depends_on": [{"task_key": "bronze_ingestion"}],
# MAGIC      "notebook_task": {"notebook_path": "/pipelines/02_silver"}},
# MAGIC     {"task_key": "gold_aggregation", "depends_on": [{"task_key": "silver_transform"}],
# MAGIC      "notebook_task": {"notebook_path": "/pipelines/03_gold"}}
# MAGIC   ],
# MAGIC   "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "Europe/Paris"}
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Alternative recommandée pour une vraie industrialisation : **Delta Live Tables (DLT)**, qui gère
# MAGIC nativement les dépendances entre couches, les attentes de qualité (`EXPECT`) et l'orchestration incrémentale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Récapitulatif
# MAGIC
# MAGIC | Couche | Tables | Lignes | Techniques clés |
# MAGIC |---|---|---|---|
# MAGIC | Bronze | customers, products, orders | brutes | Auto Loader, schéma évolutif, métadonnées d'ingestion |
# MAGIC | Silver | customers, products, orders | dédoublonnées | MERGE (upsert), typage, contraintes CHECK, partitionnement |
# MAGIC | Gold | dim_customer, dim_product, dim_date, fact_orders, vw_sales_kpis | agrégées | Modèle en étoile, OPTIMIZE/Z-ORDER, vue KPI pour Power BI |
# MAGIC
# MAGIC Ce pipeline illustre une maîtrise de bout en bout du datalakehouse Databricks : ingestion incrémentale,
# MAGIC qualité des données, modélisation dimensionnelle, performance et gouvernance — directement connectable
# MAGIC à Power BI via le connecteur natif Databricks (Import ou DirectQuery sur `vw_sales_kpis`).

# COMMAND ----------

for entity, schema in [("customers", SILVER_SCHEMA), ("products", SILVER_SCHEMA), ("orders", SILVER_SCHEMA),
                        ("dim_customer", GOLD_SCHEMA), ("dim_product", GOLD_SCHEMA),
                        ("dim_date", GOLD_SCHEMA), ("fact_orders", GOLD_SCHEMA)]:
    count = spark.table(f"{CATALOG}.{schema}.{entity}").count()
    print(f"{schema}.{entity:15s} -> {count} lignes")