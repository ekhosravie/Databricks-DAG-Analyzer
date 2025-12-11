df = spark.sql("""
SELECT
    c.c_custkey AS customer_id, c.c_name AS company_name, o.o_orderkey AS order_id,  o.o_orderdate AS order_date,  SUM(l.l_quantity * l.l_extendedprice) AS total_value,   ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate DESC) AS rn, COUNT(*) OVER () AS global_count FROM samples.tpch.customer c 
JOIN samples.tpch.orders o ON c.c_custkey = o.o_custkey 
JOIN samples.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderdate BETWEEN '1994-01-01' AND '1997-01-01'
GROUP BY c.c_custkey, c.c_name, o.o_orderkey, o.o_orderdate
HAVING SUM(l.l_quantity) > 5
ORDER BY total_value DESC
""")

# force shuffles + wide aggregations
df = df.repartition(200, "customer_id")
results = AdvancedDAGAnalyzer(df, "ComplexQueryA").analyze()
display(results)
