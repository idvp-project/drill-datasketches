# drill-datasketches
Apache Datasketches functions for Apache Drill

Currently only supports https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html

|SQL function|Aggregate|Returns|Comment|
|---|---|---|---|
|theta([column])|+|VarBinary with compacted theta sketch content|Supports various column types| 
|theta_count([column])|+|Estimated count distinct|Supports various column types|
|theta_union([VarBinary column with theta sketch])|+|VarBinary with compacted theta union sketch content||
|theta_intersection([VarBinary column with theta sketch])|+|VarBinary with compacted theta intersection sketch content||
|theta_decode([VarBinary column with theta sketch])|-|Theta value|
|theta_decode_count([VarBinary column with theta sketch])|-|Estimated count distinct|

### NOTE:
On Java 11+ it may be necessary to add value `--add-module=jdk.unsupported` to `DRILL_JAVA_OPTS`