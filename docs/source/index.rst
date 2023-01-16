.. Chronon documentation master file, created by
   sphinx-quickstart on Tue Oct 18 14:20:28 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

What is Chronon?
=====================

.. toctree::
   :hidden:

   Introduction
   Aggregations
   Integration_Guide
   Python_API
   Online_Offline_Consistency
   Code_Guidelines

Chronon is a feature engineering framework that can consume a variety of data sources and convert them into features for both training and serving. Chronon has been successfully used to power numerous business critical use-cases at Airbnb, for the last few years.

In the Chronon API you can declare:
  * **The type of Data Source to consume from** - Event streams, Fact/Dim tables in warehouse, DB table snapshots, Slowly changing dimension tables and Change Data Streams. You specify one of these types and Chronon figures out how to derive features correctly.
  * **The context to produce results in** - Online, as low-latency end-points for feature serving. Offline as hive tables, for generating training data.
  * **The time-accuracy of the computation** - Temporal or Snapshot. Temporal refers to updating feature values in real-time in an online context and producing point-in-time correct features in the offline context. Snapshot accuracy refers to features being updated once a day at midnight.
  * **The ** -
      * **GroupBy** - a powerful aggregation primitive with support for windowed and bucketed aggregations over complex types.
      * **Join** - a data gathering primitive to put together feature data from various GroupBy-s and external sources, along with labels.
      * **Staging Query** - supports running arbitrary spark sql queries with added resumability.

.. grid:: 2

    .. grid-item-card::
        :img-top: ../source/_static/index-images/getting_started.svg

        Getting Started
        ^^^^^^^^^^^^^^^

        New to Chronon? Check out the Absolute Beginner's Guide. It contains an
        introduction to Chronon's main concepts and links to additional tutorials.

        +++

        .. button-ref:: Introduction
            :expand:
            :color: secondary
            :click-parent:

            To the introduction

    .. grid-item-card::
        :img-top: ../source/_static/index-images/user_guide.svg

        User Guide
        ^^^^^^^^^^

        The user guide provides in-depth information on the
        key concepts of Chronon with useful background information and explanation.

        +++

        .. button-ref:: Integration_Guide
            :expand:
            :color: secondary
            :click-parent:

            To the user guide

    .. grid-item-card::
        :img-top: ../source/_static/index-images/api.svg

        API Reference
        ^^^^^^^^^^^^^

        The reference guide contains a detailed description of the functions,
        modules, included in Chronon. The reference describes how the
        methods work and which parameters can be used. It assumes that you have an
        understanding of the key concepts.

        +++

        .. button-ref:: Python_API
            :expand:
            :color: secondary
            :click-parent:

            To the reference guide

    .. grid-item-card::
        :img-top: ../source/_static/index-images/contributor.svg

        Contributor's Guide
        ^^^^^^^^^^^^^^^^^^^

        Want to add to the codebase? Can help add translation or a flowchart to the
        documentation? The contributing guidelines will guide you through the
        process of improving Chronon.

        +++

        .. button-ref:: Code_Guidelines
            :expand:
            :color: secondary
            :click-parent:

            To the contributor's guide