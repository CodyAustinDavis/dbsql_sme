# Contributing
Contributions are welcome! Feel free to file an issue/PR or reach out to michael.berk@databricks.com.

### Potential Contributions (in order of importance)
* Support UC
  * Support writing raw queries to UC volumes instead of DBFS
  * Modify existing data write to handle conversion to UC-managed tables
* Scope warehouse concurrency limitations
* Improve beaker performance calculations - [issue](https://github.com/goodwillpunning/beaker/issues/24)
* Look to improve data write performance. Some options include:
  * Improve threading of writes.
  * Document baseline TPC-DS benchmarking runtimes - [template](https://github.com/databricks/spark-sql-perf/blob/master/src/main/notebooks/tpcds_datagen.scala)
* Allow for running create data OR run benchmarking. Currently these methods are coupled and it prevent's rerunning different warehouse benchmarks against the same data source
* Add dashboarding and further analysis using Nishant's tool(s)
* Determine if spark-sql-perf supports latest LTS DBR version or if we need to hardcode 12.2
* Make Beaker pip-installable within a Databricks notebook then remove the hard-coded .whl url - [issue](https://github.com/goodwillpunning/beaker/issues/19)
