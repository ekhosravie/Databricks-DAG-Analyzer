Databricks DAG Analyzer:
A complete Spark execution plan deep-analysis engine for Databricks.
This tool parses Spark’s logical, physical, formatted, and JSON execution plans and performs 60+ performance checks across shuffles, joins, skew, partitioning, cardinality, physical operator behavior, and cluster resource usage.

It is built for:

Data Engineers
Performance Engineers
Architects
Databricks Optimization Teams

✨ Key Features
Deep Execution Plan Understanding
Tree-based plan parsing
JSON operator analysis resilient to DBR version changes
Extracts shuffles, joins, window functions, aggregations, sorting, expands, exchanges, AQE transformations

📊 Automated Bottleneck Detection
Over 60 built-in detection rules:
Skew
Bad repartitioning patterns
Heavy SMJ (Sort Merge Join)
Improper coalesce
Wide table warnings
High-cardinality columns
Small file fragmentation
Large shuffle stages
Window heavy pipelines
Rebalance / Repartition misuse
Cluster-Aware Recommendations
Recommended number of partitions
Optimized join strategy
Broadcast feasibility
Memory-spill risk analysis
Shuffle-volume severity

📈 Structured Output
Results come as a Spark DataFrame:
category
severity
description
recommendation
impact
estimated impact %
priority








Architecture — Step-by-Step Execution Flow

Below is the full lifecycle of the analyzer.

Step 1 — Capture Stats

When the user calls:

AdvancedDAGAnalyzer(df, "MyQuery").analyze()

The analyzer first collects:

Row count (approx)
Columns count
Schema structure
Numeric column distributions
Cluster specs
Partition counts
File count + file sizes
This creates the baseline profile for the dataset.

Step 2 — Extract Execution Plans

The analyzer extracts all executions plans:

Logical plan
df.queryExecution.logical

Physical plan
df.queryExecution.executedPlan.toString()

Formatted plan
spark.sql("EXPLAIN FORMATTED ...")

JSON plan (critical)
df.queryExecution.executedPlan.toJSON()


➡️ This is where shuffle, sort, exchange, repartition, window, and join operators are extracted.

Step 3 — Convert Plan JSON to a Tree

Your analyzer performs recursive parsing:

Scan → Filter → Project → Shuffle → Join → Window → Sort → Exchange → Write

Each node is classified into:
Shuffle
Join
Sort
Window
Exchange
Repartition
Broadcast
Agg

This creates a stable operator tree across all Spark versions.

Step 4 — Count Critical Operators

The _count_operators_in_tree function identifies:

Operator	Why It Matters
Shuffle	Most expensive operation in Spark
Sort	Spill-heavy, expensive
Window	State-heavy
Join	SMJ/BHJ decision check
Aggregate	Causes shuffle + wide dependencies
Exchange	Partition movement
Repartition	Expensive when misused

Each operator is counted globally and per-branch.

Step 5 — Detect Bottlenecks (60 Rules)

This includes:

Skew Detection
Median vs max partition size
10x skew ratio triggers warning
Recommendations: salting, repartitioning, using AQE skew hints
Join Strategy Analysis
Does plan use SMJ, BHJ, SHJ?

Is it optimal?
Should broadcasting be forced?

Shuffle Explosion
Shuffle read size
Shuffle write size
Exchange patterns
Wide Tables
250 columns
Nested types
Recommendation: prune/select early
High Cardinality
Top 3 columns
Impact on grouping, join keys
Small Files
Total size vs file count
Recommendation: OPTIMIZE + ZORDER
Sorting Risks
SortMergeJoin preparation
ORDER BY requiring full shuffle
Memory Pressure
Estimation of row size
Partition memory requirements
Spill probability
Step 6 — Severity Scoring
The analyzer assigns:
severity (0–3)
impact level
priority (1–60)

This makes the output sortable and comparable across jobs.

Step 7 — Generate Final DataFrame

Columns:
priority
category
severity
description
recommendation
impact
estimated_impact_pct
query_name
plan_type
operator_type

Step 8 — Return Results

The user receives a fully analyzable Spark DataFrame, ready for:
Logging into Delta tables
Daily monitoring
Performance regression tracking
