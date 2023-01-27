.. Chronon documentation master file, created by
   sphinx-quickstart on Tue Oct 18 14:20:28 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 2
   :hidden:

   Getting_Started
   Concepts
   Aggregations
   Python_API
   Kaggle_Outbrain
   Online_Offline_Consistency
   Code_Guidelines

What ?
=====================
Chronon is a fast, powerful, production-grade feature engineering framework. Chronon can generate pipelines that produce training data, low-latency end-points for serving features and generate metrics computation pipelines for monitoring data quality. Chronon has been used to power various business-critical use-cases.

* Consume data from a variety of Sources - Event streams, Fact/Dim tables in warehouse, DB table snapshots, Slowly changing dimension tables, Change Data Streams, etc.
* Produce results both online and offline - Online, as low-latency end-points for feature serving, or Offline as hive tables, for generating training data.
* That can be updated in real time: You can configure the accuracy of the result  to be either Temporal or Snapshot. Temporal refers to updating feature values in real-time in an online context and producing point-in-time correct features in the offline context. Snapshot accuracy refers to features being updated once a day at midnight.
* With a powerful python API - which adds time and windowing  along with the above mentioned concepts as first class ideas to the familiar SQL primitives like group-by, join, select etc, while retaining the full flexibility and composability offered by Python.

Being able to flexibly compose these concepts to describe data processing is what makes feature engineering in Chronon productive.

Example
=====================
This is what a simple Chronon Group-By looks like.

.. code-block:: python

    # same definition creates offline datasets and online end-points
    view_features = GroupBy(
       sources=[
           EventSource(
               # apply the transform on offline and streaming data
               table="user_activity.user_views_table",
               topic="user_views_stream",
               query=query.Query(
                   # specify any spark sql expression fragments
                   # built-in functions, UDFs, arithmetic operations, inline-lambdas, struct types etc.
                   selects={
                       "view": "if(context['activity_type'] = 'item_view', 1 , 0)",
                   },
                   wheres=["user != null"]
               ))
       ],
       # composite keys
       keys=["user", "item"],
       aggregations=[
           Aggregation(
               operation=Operation.COUNT,
               # automatically explode aggregation list type input columns
               input_column=view,
               #multiple windows for the same input
               windows=[Window(length=5, timeUnit=TimeUnit.HOURS)]),
       ],
       # toggle between fresh vs daily updated features
       accuracy=Accuracy.TEMPORAL,
    )

Getting Started
=====================
If you wish to work in an existing chronon repo, simply run and checkout

.. code-block:: bash

   pip install chronon-ai

If you wish to setup a chronon repo and install all the necessary packages, simply run

.. code-block:: bash

   source <(curl -s https://storage.googleapis.com/chronon/releases/init.sh)

Once you edit the spark_submit_path line in :code:`./chronon/teams.json` you will be able to run offline jobs.
Find more details in the Getting Started section.