Understanding the Output Columns

The analyzer returns a structured Spark DataFrame summarizing all detected optimization signals.
Each row represents one identified insight, risk, or bottleneck in the DAG.

Below is the full explanation of each column and how it should be interpreted.

1. priority

Type: Integer
Purpose: Controls sorting order of insights

What It Means

This is the rank assigned to each finding based on its importance.
Lower numbers = higher priority.

Priority Range	Meaning
1–10	Critical bottlenecks requiring immediate attention
11–25	High importance performance improvements
26–45	Moderate tuning opportunities
46–60	Low impact improvements or informational notes

How It’s Used

Sorting results
Alerts or thresholds


2. category

Type: String
Purpose: Groups findings into top-level optimization areas

Example Categories
Shuffle Optimization
Join Strategy
Skew Detection
Partitioning
Memory Pressure
File Layout
Wide Table Warning
Window Functions
Cluster Tuning
Scan/IO Efficiency

Why It Matters

It makes the output readable and lets users quickly scan issues per theme.

3. severity

Type: Integer (0–3)
Purpose: Severity of the issue found

Level	Meaning
0	Healthy / No issue
1	Minor inefficiency
2	Warning — potential performance problem
3	Critical bottleneck

How Severity Is Calculated

Based on:

Shuffle volume
Skew ratio
Operator cost
Join type correctness
Estimated memory pressure
Output explosion risk

4. description

Type: String
Purpose: Human-readable summary of the issue

Examples

"Detected large ShuffleExchange between Scan and SMJ"
"Join uses SortMergeJoin but tables are small enough for broadcast"
"Significant partition skew detected: max partition 14.8× median"
"WindowExec operator chaining detected — potential high memory usage"

Notes

This text is meant to be clear, direct, and interpretable by both beginners and senior engineers.

5. recommendation

Type: String
Purpose: Actionable tuning advice to address the issue

Examples

"Consider broadcasting the right table using broadcast() or join hint"
"Use repartitionByRange on join keys to reduce shuffle volume"
"Optimize small files with OPTIMIZE ZORDER"
"Apply salting or key bucketing to reduce skew"
"Push down filters earlier to reduce scanned rows"

Why It Matters

This is the core value of the analyzer — turning problems into solutions.

6. impact

Type: String
Purpose: Qualitative estimate of how much the issue affects performance

Typical Values
"Low"
"Moderate"
"High"
"Very High"
"Critical"

Interpretation

This reflects problem cost, not just severity.
A minor shuffle could be "Low" impact.
A massive SMJ on unpartitioned data can be "Critical".

7. estimated_impact_pct

Type: Float (%)
Purpose: Rough performance penalty estimation

Example Values

5.2 → approx 5% performance penalty
23.8 → approx 24% slower
55.0 → huge cost from skew or shuffles

How It’s Calculated

Based on:

Shuffle read/write volume
Number of exchanges
Operator cardinality expansion
Window memory footprint
Skew ratio
File fragmentation

This is not a precise runtime prediction — it's a relative cost indicator useful for ranking issues.

8. query_name

Type: String
Purpose: User-friendly tag for which query or DataFrame the results belong to

Why It's Important

Allows tracking across:
Daily ETL DAG monitoring
Regression testing
Multi-query benchmark reports

Example

If you call:

AdvancedDAGAnalyzer(df, "CustomerOrderJoin")


Your output contains "CustomerOrderJoin" in every row.

9. plan_type

Type: String
Purpose: Which execution plan contributed to the finding

Possible Values

"logical"
"physical"
"formatted"
"json"

Explanation

Some insights come from:

logical plan (filters, projections, nested subqueries)
physical plan (SMJ, BHJ, Repartition, WindowExec)
JSON plan (low-level operator metadata)
formatted (IO + cost info)

This helps analysts debug where the insight originated.

10. operator_type

Type: String
Purpose: The Spark operator responsible for the finding

Examples

"ShuffleExchangeExec"
"SortMergeJoinExec"
"BroadcastHashJoinExec"
"WindowExec"
"SortExec"
"Repartition"
"Aggregate"
"Scan"

Why It Matters

Users can connect findings to actual DAG nodes and visualize:

Which operator caused the issue

Where in the plan it occurred

Whether it’s part of AQE transformations

It also helps engineers understand the real source of performance problems.
