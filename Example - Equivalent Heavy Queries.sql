SELECT
    c.c_custkey,
    c.c_name,
    o.o_orderkey,
    o.o_orderdate,
    SUM(l.l_extendedprice * l.l_quantity) AS total_value,
    ROW_NUMBER() OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate DESC) AS rn
FROM samples.tpch.customer AS c
JOIN samples.tpch.orders AS o
    ON c.c_custkey = o.o_custkey
JOIN samples.tpch.lineitem AS l
    ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderdate BETWEEN '1994-01-01' AND '1997-01-01'
GROUP BY c.c_custkey, c.c_name, o.o_orderkey, o.o_orderdate
HAVING SUM(l.l_quantity) > 5
ORDER BY total_value DESC;
