spark.sql("""
WITH base AS (
    SELECT
        l.order_id,
        l.extendedprice,
        l.quantity,
        o.orderdate,
        c.nationkey
    FROM samples.tpch.lineitem l
    JOIN samples.tpch.orders o ON l.order_id = o.order_id
    JOIN samples.tpch.customer c ON c.customer_id = o.customer_id
),
win AS (
    SELECT *,
        AVG(extendedprice) OVER (PARTITION BY nationkey) AS avg_price_nation,
        SUM(quantity) OVER (ORDER BY orderdate ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS rolling_qty
    FROM base
)
SELECT * FROM win WHERE avg_price_nation > 1000
""").createOrReplaceTempView("complex_win")

df = spark.sql("SELECT * FROM complex_win WHERE rolling_qty > 200")

results = AdvancedDAGAnalyzer(df, "ComplexWindowQuery").analyze()
results.show(truncate=False)
