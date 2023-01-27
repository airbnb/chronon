namespace py api
namespace java ai.chronon.api

// cd /path/to/chronon
// thrift --gen py -out api/py/ai/chronon api/thrift/api.thrift

struct Query {
    1: optional map<string, string> selects
    2: optional list<string> wheres
    3: optional string startPartition
    4: optional string endPartition
    5: optional string timeColumn
    6: optional list<string> setups = []
    7: optional string mutationTimeColumn
    8: optional string reversalColumn
}

/**
    Staging Query encapsulates arbitrary spark computation. One key feature is that the computation follows a
    "fill-what's-missing" pattern. Basically instead of explicitly specifying dates you specify two macros.
    `{{ start_date }}` and `{{end_date}}`. Chronon will pass in earliest-missing-partition for `start_date` and
    execution-date / today for `end_date`. So the query will compute multiple partitions at once.
 */
struct StagingQuery {
    /**
    * Contains name, team, output_namespace, execution parameters etc. Things that don't change the semantics of the computation itself.
    **/
    1: optional MetaData metaData

    /**
    * Arbitrary spark query that should be written with `{{ start_date }}`, `{{ end_date }}` and `{{ latest_date }}` templates
    *      - `{{ start_date }}` will be set to this user provided start date, future incremental runs will set it to the latest existing partition + 1 day.
    *      - `{{ end_date }}` is the end partition of the computing range.
    *      - `{{ latest_date }}` is the end partition independent of the computing range (meant for cumulative sources).
    **/
    2: optional string query

    /**
    * on the first run, `{{ start_date }}` will be set to this user provided start date, future incremental runs will set it to the latest existing partition + 1 day.
    **/
    3: optional string startPartition

    /**
    * Spark SQL setup statements. Used typically to register UDFs.
    **/
    4: optional list<string> setups
}

struct EventSource {
    /**
    * Table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table. Table names can contain subpartition specs, example db.table/system=mobile/currency=USD
    **/
    1: optional string table

    /**
    * Topic is a kafka table. The table contains all the events historically came through this topic.
    **/
    2: optional string topic

    /**
    * The logic used to scan both the table and the topic. Contains row level transformations and filtering expressed as Spark SQL statements.
    **/
    3: optional Query query

    /**
    * If each new hive partition contains not just the current day's events but the entire set of events since the begininng. The key property is that the events are not mutated across partitions.
    **/
    4: optional bool isCumulative
}


/**
    Entity Sources represent data that gets mutated over-time - at row-level. This is a group of three data elements.
    snapshotTable, mutationTable and mutationTopic. mutationTable and mutationTopic are only necessary if we are trying
    to create realtime or point-in-time aggregations over these sources. Entity sources usually map 1:1 with a database
    tables in your OLTP store that typically serves live application traffic. When mutation data is absent they map 1:1
    to `dim` tables in star schema.
 */
struct EntitySource {
    /**
    Snapshot table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table.
    */
    1: optional string snapshotTable

    /**
    Topic is a kafka table. The table contains all the events that historically came through this topic.
    */
    2: optional string mutationTable

    /**
    The logic used to scan both the table and the topic. Contains row level transformations and filtering expressed as Spark SQL statements.
    */
    3: optional string mutationTopic

    /**
    If each new hive partition contains not just the current day's events but the entire set of events since the begininng. The key property is that the events are not mutated across partitions.
    */
    4: optional Query query
}

struct ExternalSource {
    1: optional MetaData metadata
    2: optional TDataType keySchema
    3: optional TDataType valueSchema
}

union Source {
    1: EventSource events
    2: EntitySource entities
}

enum Operation {
    // Un-Deletable operations - Monoids
    // Once an aggregate is created from a group of elements,
    // asking to delete on of the elements is invalid - and ignored.
    MIN = 0
    MAX = 1
    FIRST = 2
    LAST = 3
    UNIQUE_COUNT = 4
    APPROX_UNIQUE_COUNT = 5

    // Deletable operations - Abelian Groups
    // Once an aggregate is created from a group of elements,
    // deletion of any particular element can be done freely.
    COUNT = 6
    SUM = 7
    AVERAGE = 8
    VARIANCE = 9
    SKEW = 10     // TODO
    KURTOSIS = 11  // TODO
    APPROX_PERCENTILE = 12

    LAST_K = 13
    FIRST_K = 14,
    TOP_K = 15,
    BOTTOM_K = 16

    HISTOGRAM = 17 // use this only if you know the set of inputs is bounded
}

// integers map to milliseconds in the timeunit
enum TimeUnit {
    HOURS = 0
    DAYS = 1
}

struct Window {
    1: i32 length
    2: TimeUnit timeUnit
}

/**
    Chronon provides a powerful aggregations primitive - that takes the familiar aggregation operation, via groupBy in
    SQL and extends it with three things - windowing, bucketing and auto-explode.
 */
struct Aggregation {
    /**
    *  The column as specified in source.query.selects - on which we need to aggregate with.
    **/
    1: optional string inputColumn
    /**
    * The type of aggregation that needs to be performed on the inputColumn.
    **/
    2: optional Operation operation
    /**
    * Extra arguments that needs to be passed to some of the operations like LAST_K, APPROX_PERCENTILE.
    **/
    3: optional map<string, string> argMap

    /**
    For TEMPORAL case windows are sawtooth. Meaning head slides ahead continuously in time, whereas, the tail only hops ahead, at discrete points in time. Hop is determined by the window size automatically. The maximum hop size is 1/12 of window size. You can specify multiple such windows at once.
      - Window > 12 days  -> Hop Size = 1 day
      - Window > 12 hours -> Hop Size = 1 hr
      - Window > 1hr      -> Hop Size = 5 minutes
    */
    4: optional list<Window> windows

    /**
    This is an additional layer of aggregation. You can key a group_by by user, and bucket a “item_view” count by “item_category”. This will produce one row per user, with column containing map of “item_category” to “view_count”. You can specify multiple such buckets at once
    */
    5: optional list<string> buckets
}

// used internally not exposed - maps 1:1 with a field in the output
struct AggregationPart {
    1: optional string inputColumn
    2: optional Operation operation
    3: optional map<string, string> argMap
    4: optional Window window
    5: optional string bucket
}

enum Accuracy {
    TEMPORAL = 0,
    SNAPSHOT = 1
}

//TODO: to be supported
//enum JoinType {
//    OUTER = 0,
//    INNER = 1
//}

struct MetaData {
    1: optional string name
    // marking this as true means that the conf can be served online
    // once marked online, a conf cannot be changed - compiling the conf won't be allowed
    2: optional bool online
    // marking this as true means that the conf automatically generates a staging copy
    // this flag is also meant to help a monitoring system re-direct alerts appropriately
    3: optional bool production
    4: optional string customJson
    5: optional list<string> dependencies
    6: optional map<string, string> tableProperties
    // todo: add sanity check in materialize script
    7: optional string outputNamespace
    // team name for the job
    8: optional string team
    // modes - backfill, upload, streaming
    // join streaming makes sense & join upload probably also makes sense
    // (These just aren't implemented yet)
    // The inner map should contain environment variables
    9: optional map<string, map<string, string>> modeToEnvMap
    10: optional bool consistencyCheck
    11: optional double samplePercent
    // cron expression for airflow DAG schedule
    12: optional string offlineSchedule
}

// Equivalent to a FeatureSet in chronon terms
struct GroupBy {
    1: optional MetaData metaData
    // CONDITION: all sources select the same columns
    // source[i].select.keys() == source[0].select.keys()
    2: optional list<Source> sources
    // CONDITION: all keyColumns are selected by each of the
    // set(sources[0].select.keys()) <= set(keyColumns)
    3: optional list<string> keyColumns
    // when aggregations are not specified,
    // we assume the source is already grouped by keys
    4: optional list<Aggregation> aggregations
    5: optional Accuracy accuracy
    // Optional start date for a group by backfill, if it's unset then no historical partitions will be generate
    6: optional string backfillStartDate
}

struct AggregationSelector {
  1: optional string name
  2: optional list<Window> windows
}

struct JoinPart {
    1: optional GroupBy groupBy
    2: optional map<string, string> keyMapping
    3: optional list<AggregationSelector> selectors # deprecated
    4: optional string prefix
}

struct ExternalPart {
    1: optional ExternalSource source
    // what keys on the left becomes what keys in the external source
    // currently this only supports renaming, in the future this will run catalyst expressions
    2: optional map<string, string> keyMapping
    3: optional string prefix
}

// A Temporal join - with a root source, with multiple groupby's.
struct Join {
    1: optional MetaData metaData
    2: optional Source left
    3: list<JoinPart> joinParts
    // map of left key column name and values representing the skew keys
    // these skew keys will be excluded from the output
    // specifying skew keys will also help us scan less raw data before aggregation & join
    // example: {"zipcode": ["94107", "78934"], "country": ["'US'", "'IN'"]}
    4: optional map<string,list<string>> skewKeys
    // users can register external sources into Api implementation. Chronon fetcher can invoke the implementation.
    // This is applicable only for online fetching. Offline this will not be produce any values.
    5: optional list<ExternalPart> onlineExternalParts
    6: optional LabelPart labelPart
}

// Label join parts and params
struct LabelPart {
    1: optional list<JoinPart> labels
    // The earliest date label should be refreshed
    2: optional i32 leftStartOffset
    // The most rencet date label should be refreshed.
    // e.g. left_end_offset = 3 most recent label available will be 3 days prior to 'label_ds'
    3: optional i32 leftEndOffset
//    4: optional JoinType joinType
}

// This is written by the bulk upload process into the metaDataset
// streaming uses this to
//     1. gather inputSchema from the kafka stream
//     2. to check if the groupByJson is the same as the one it received - and emits a
struct GroupByServingInfo {
    1: optional GroupBy groupBy
    // a. When groupBy is
    //  1. temporal accurate - batch uploads irs, streaming uploads inputs
    //                         the fetcher further aggregates irs and inputs into outputs
    //  2. snapshot accurate - batch uploads outputs, fetcher doesn't do any further aggregation
    // irSchema and outputSchema are derivable once inputSchema is known


    // schema before applying select expressions
    2: optional string inputAvroSchema
    // schema after applying select expressions
    3: optional string selectedAvroSchema
    // schema of the keys in kv store
    4: optional string keyAvroSchema
    // "date", in 'yyyy-MM-dd' format, the bulk-upload data corresponds to
    // we need to scan streaming events only starting this timestamp
    // used to compute
    //       1. batch_data_lag = current_time - batch_data_time
    //       2. batch_upload_lag = batch_upload_time - batch_data_time
    5: optional string batchEndDate
}

// DataKind + TypeParams = DataType
// for primitive types there is no need for params
enum DataKind {
    // non parametric types
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    DATE = 9,
    TIMESTAMP = 10,

    // parametric types
    MAP = 11,
    LIST = 12,
    STRUCT = 13,
}

struct DataField {
    1: optional string name
    2: optional TDataType dataType
}

// TDataType because DataType has idiomatic implementation in scala and py
struct TDataType {
    1: DataKind kind
    2: optional list<DataField> params
    3: optional string name // required only for struct types
}