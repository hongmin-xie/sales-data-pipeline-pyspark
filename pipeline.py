from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]

def aggregate(sales, groups=()):
    return (sales.groupBy(*groups).agg(
        F.sum("quantity").alias("units"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("revenue").alias("revenue"),
        F.sum("cost").alias("cost"))
        .withColumn("margin", F.col("revenue")-F.col("cost"))
        .withColumn("markup_pct", F.when(F.col("cost") != 0, F.round(100*F.col("margin")/F.col("cost"),2)))
        .withColumn("margin_pct", F.when(F.col("revenue") != 0, F.round(100*F.col("margin")/F.col("revenue"),2))))

def pipeline(spark, data_dir):
    orders = spark.read.csv(str(data_dir / "orders_raw.csv"), header=True).withColumn("sku", F.trim("sku"))
    products = spark.read.csv(str(data_dir / "product_ref.csv"), header=True).withColumn("sku", F.trim("sku"))
    if products.filter(F.col("sku").isNull() | (F.col("sku") == "")).count() or products.groupBy("sku").count().filter("count > 1").count():
        raise ValueError("Product keys must be nonempty and unique")
    date = F.trim("order_date")
    normalized_date = F.coalesce(*[F.try_to_timestamp(date, F.lit(fmt)).cast("date") for fmt in ("yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy", "dd-MM-yyyy")])
    amount = F.regexp_replace(F.trim("amount"), r"[\s\u00a0\u202f]", "")
    amount = (F.when(amount.rlike(r"^\d{1,3}(,\d{3})+\.\d{2}$"), F.regexp_replace(amount, ",", ""))
        .when(amount.rlike(r"^\d{1,3}(\.\d{3})+,\d{2}$"), F.regexp_replace(F.regexp_replace(amount, r"\.", ""), ",", "."))
        .otherwise(F.regexp_replace(amount, ",", ".")))
    valid = (orders.withColumn("date", normalized_date).withColumn("normalized_amount", amount)
        .withColumn("revenue", F.expr("try_cast(normalized_amount as decimal(18,2))"))
        .withColumn("quantity", F.expr("try_cast(qty as int)"))
        .filter(F.col("date").between("2025-01-01", "2025-01-31") & (F.col("currency") == "EUR") & F.col("revenue").isNotNull() & (F.col("quantity") > 0)))
    missing = valid.join(products.select("sku"), "sku", "left_anti").count()
    products = products.withColumn("unit_cost", F.expr("try_cast(unit_cost_eur as decimal(18,2))"))
    sales = (valid.join(F.broadcast(products), "sku", "inner").filter(F.col("unit_cost").isNotNull())
        .withColumn("cost", (F.col("quantity") * F.col("unit_cost")).cast("decimal(18,2)")))
    overall, categories = aggregate(sales), aggregate(sales, ["category"])
    total = overall.first()
    subtotal = categories.agg(*[F.sum(k).alias(k) for k in ("units", "revenue", "cost")]).first()
    for k in ("units", "revenue", "cost"):
        if total[k] != subtotal[k]:
            raise AssertionError(f"Reconciliation failed: {k}")
    print(f"Source rows: {orders.count()}; unmatched rows: {missing}; retained rows: {sales.count()}")
    return overall, categories

def main():
    spark = SparkSession.builder.master("local[2]").appName("RetailSalesPipeline").getOrCreate()
    try:
        overall, categories = pipeline(spark, ROOT / "data")
        overall.show(truncate=False)
        categories.show(truncate=False)
        for name, frame in [("global", overall), ("by_category", categories)]:
            frame.coalesce(1).write.mode("overwrite").option("header", True).csv(str(ROOT / "outputs" / name))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
