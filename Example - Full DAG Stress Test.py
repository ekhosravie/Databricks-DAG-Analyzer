df = spark.sql("""
WITH a AS (
    SELECT * FROM samples.tpch.customer
),
b AS (
    SELECT *, dense_rank() OVER (ORDER BY o_orderpriority) AS rk
    FROM samples.tpch.orders
),
c AS (
    SELECT * FROM samples.tpch.lineitem WHERE l_discount > 0.05
)
SELECT
    a.c_name,
    b.o_orderkey,
    SUM(c.l_extendedprice) AS total_price,
    COUNT(*) OVER () AS global_cnt
FROM a
JOIN b ON a.c_custkey = b.o_custkey
JOIN c ON b.o_orderkey = c.l_orderkey
GROUP BY a.c_name, b.o_orderkey
ORDER BY total_price DESC
""")

df = df.repartition(300)

results = AdvancedDAGAnalyzer(df, "FullStressTest").analyze()
results.show(truncate=False)
