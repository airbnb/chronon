namespace py ai.zipline.api
namespace java ai.zipline.api

// to manually generate py defs
// cd path/to/bighead/zipline/python/zipline/schema
// thrift --gen py -out . path/to/bighead/thrift/schemas/zipline_api.thrift;  popd
// TODO integrate thrit generation step into sbt
struct Query {
    1: optional map<string, string> select
    2: optional string where
    3: optional string startPartition
    4: optional string endPartition
    5: optional string timeColumn
    6: optional list<string> setups = []
    7: optional string mutationTimeColumn
    8: optional string reversalColumn
}


struct StagingQuery {
    1: optional string name
    // query should be written with `{{ start_date }}` and `{{ end_date }}` templates
    2: optional string query
    // on the first run, `{{ start_date }}` will be set to this user provided start date, future incremental runs will
    // set it to the latest existing partition + 1 day
    3: optional string startDate
    4: optional list<string> dependencies
    5: optional list<string> querySetupCommands
}

struct EventSource {
    1: optional string table
    2: optional string topic
    3: optional Query query
}

struct EntitySource {
    1: optional string snapshotTable
    2: optional string mutationTable
    3: optional string mutationTopic
    4: optional Query query
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
    TOP = 4
    BOTTOM = 5
    APPROX_UNIQUE_COUNT = 6

    // Deletable operations - Abelian Groups
    // Once an aggregate is created from a group of elements,
    // deletion of any particular element can be done freely.
    COUNT = 7
    SUM = 8
    AVERAGE = 9
    VARIANCE = 10  // TODO
    SKEW = 11     // TODO
    KURTOSIS = 12  // TODO
    APPROX_PERCENTILE = 13 // TODO

    LAST_K = 14
    FIRST_K = 15,
    TOP_K = 16,
    BOTTOM_K = 17
}

// integers map to milliseconds in the timeunit
enum TimeUnit {
    MILLISECONDS = 1
    SECONDS = 1000
    MINUTES = 60000
    HOURS = 3600000
    DAYS = 86400000
}

struct Window {
    1: i32 length
    2: TimeUnit timeUnit
}

//Feature
struct Aggregation {
    1: optional string inputColumn
    2: optional Operation operation
    3: optional map<string, string> argsJson
    4: optional list<Window> windows
}

// Equivalent to a FeatureSet in zipline terms
struct GroupBy {
    1: optional MetaData metadata
    // CONDITION: all sources select the same columns
    // source[i].select.keys() == source[0].select.keys()
    2: optional list<Source> sources
    // CONDITION: all keyColumns are selected by each of the
    // set(sources[0].select.keys()) <= set(keyColumns)
    3: optional list<string> keyColumns
    // when aggregations are not specified,
    // we assume the source is already grouped by keys
    4: optional list<Aggregation> aggregations
}

struct AggregationSelector {
  1: optional string name
  2: optional list<Window> windows
}

struct JoinPart {
    1: optional GroupBy groupBy
    2: optional map<string, string> keyMapping
    3: optional list<AggregationSelector> selectors
    4: optional string prefix
}

struct MetaData {
    1: optional string name
    2: optional bool online
    3: optional bool production
    4: optional string customJson
    5: optional list<string> dependencies
}


// A Temporal join - with a root source, with multiple groupby's.
struct Join {
    1: optional MetaData metaData
    2: optional Source left
    3: list<JoinPart> joinParts
}
