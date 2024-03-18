# Databricks notebook source
# MAGIC %md
# MAGIC ## Licence
# MAGIC See LICENSE file.
# MAGIC
# MAGIC ## Data collection
# MAGIC To improve users experience and dbdemos asset quality, dbdemos sends report usage and capture views in the installed notebook (usually in the first cell) and other assets like dashboards. This information is captured for product improvement only and not for marketing purpose, and doesn't contain PII information. By using `dbdemos` and the assets it provides, you consent to this data collection. If you wish to disable it, you can set `Tracker.enable_tracker` to False in the `tracker.py` file.
# MAGIC
# MAGIC ## Resource creation
# MAGIC To simplify your experience, `dbdemos` will create and start for you resources. As example, a demo could start (not exhaustive):
# MAGIC - A cluster to run your demo
# MAGIC - A Delta Live Table Pipeline to ingest data
# MAGIC - A DBSQL endpoint to run DBSQL dashboard
# MAGIC - An ML model
# MAGIC
# MAGIC While `dbdemos` does its best to limit the consumption and enforce resource auto-termination, you remain responsible for the resources created and the potential consumption associated.
# MAGIC
# MAGIC ## Support
# MAGIC Databricks does not offer official support for `dbdemos` and the associated assets.
# MAGIC For any issue with `dbdemos` or the demos installed, please open an issue and the demo team will have a look on a best effort basis.
# MAGIC
# MAGIC
