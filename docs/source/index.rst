.. Chronon documentation master file, created by
   sphinx-quickstart on Tue Oct 18 14:20:28 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Introduction

   getting_started/Introduction
   getting_started/Tutorial

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Creating Training data

   authoring_features/GroupBy
   authoring_features/Join
   authoring_features/ChainingFeatures
   authoring_features/Source
   authoring_features/StagingQuery
   authoring_features/DerivedFeatures
   authoring_features/Bootstrap

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Setup

   setup/Overview
   setup/Components
   setup/Data_Integration
   setup/Developer_Setup
   setup/Online_Integration
   setup/Orchestration
   setup/Flink

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Test, Serve & Monitor

   test_deploy_serve/Test
   test_deploy_serve/Deploy
   test_deploy_serve/Serve
   test_deploy_serve/Online_Offline_Consistency

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: More References

   Python_API
   Tiled_Architecture
   Code_Guidelines
   Kaggle_Outbrain


.. |br| raw:: html

   <br />

What is Chronon?
=====================
Chronon is an open source end-to-end feature platform that allows Machine Learning (ML) teams to easily build, deploy, manage and monitor data pipelines for machine learning.

It's currently used to power all major ML applications within Airbnb, as well as major use cases at Stripe. Airbnb and Stripe jointly manage and maintain the project, and welcome your usage and contributions!

.. image:: ../images/chronon_high_level.png

|br|

Key Features
=====================

* **Consume data from a variety of Sources** - event streams, DB table snapshots, change data streams, service endpoints and warehouse tables modeled as either slowly changing dimensions, fact or dimension tables
* **Produce results both online and offline contexts** - Online, as scalable low-latency end-points for feature serving, or offline as hive tables, for generating training data.
* **Real-time or batch accuracy** - You can configure the result to be either *Temporal* or *Snapshot* accurate. Temporal refers to updating feature values in real-time in online context and producing point-in-time correct features in the offline context. Snapshot accuracy refers to features being updated once a day at midnight.
* **Backfill training sets from raw data** - without having to wait for months to accumulate feature logs to train your model.
* **Powerful python API** - data source types, freshness and contexts are API level abstractions that you compose with intuitive SQL primitives like group-by, join, select etc., with powerful enhancements.
* **Automated feature monitoring** - auto-generate monitoring pipelines to understand training data quality, measure training-serving skew and monitor feature drift.

|br|

Example
=====================
Here is a code example showing what a simple Chronon GroupBy looks like. 

This definition starts with purchase events as the raw input source, and creates user level features by aggregating the number of purchases and the purchase value in various windows, using various aggregations. This single definition can be used to automatically create offline datasets, feature serving end-points and data quality monitoring pipelines.

.. code-block:: python

   """
   This GroupBy aggregates metrics about a user's previous purchases in various windows.
   """

   # This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
   source = Source(
      events=EventSource(
         table="data.purchases", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
         topic= "events/purchases", # The streaming source topic that can be listened to for realtime events
         query=Query(
               selects=select(
                  user="user_id",
                  price="purchase_price * (1 - merchant_fee_percent/100)"
               ), # Select the fields we care about
               time_column="ts"  # The event time
         ) 
      )
   )

   window_sizes = [Window(length=day, timeUnit=TimeUnit.DAYS) for day in [3, 14, 30]] # Define some window sizes to use below

   v1 = GroupBy(
      sources=[source],
      keys=["user_id"], # We are aggregating by user
      online=True,
      aggregations=[Aggregation(
               input_column="price",
               operation=Operation.SUM,
               windows=window_sizes
         ), # The sum of purchases prices
         Aggregation(
               input_column="price",
               operation=Operation.COUNT,
               windows=window_sizes
         ), # The count of purchases
         Aggregation(
               input_column="price",
               operation=Operation.AVERAGE,
               windows=window_sizes
         ), # The average purchases
         Aggregation(
               input_column="price",
               operation=Operation.LAST_K(10),
         ), # The last 10 purchase prices, collected into a list
      ], # All aggregations are performed over the window_sizes defined above
   )

To run this and other features and see the complete flow from generating training data to online serving, continue along to the `Quickstart Tutorial <https://chronon.ai/getting_started/Tutorial.html>`_, or for more documentation on how to author and use features, see the `Creating Training Data <https://chronon.ai/authoring_features/GroupBy.html>`_ section.
