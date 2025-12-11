from pyspark.sql.functions import lit

# Make skew by forcing 90% of rows into one key
lineitem_skew = spark.table("samples.tpch.lineitem") \
    .withColumn("skew_key", lit(1))

df = lineitem_skew.groupBy("skew_key").agg({"quantity": "sum"})

results = AdvancedDAGAnalyzer(df, "SkewTest").analyze()
results.show(truncate=False)
