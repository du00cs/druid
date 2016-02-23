Avro Extensions

# 1. Avro Hadoop Ingestion

# 2. Avro Streaming Ingestion

# 3. Parquet Hadoop Ingestion

It is no need to make your cluster to update to SNAPSHOT, you can just fire a hadoop job with your local compiled jars like:

```bash
HADOOP_CLASS_PATH=`hadoop classpath | sed s/*.jar/*/g`

java -Xmx32m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
  -classpath config/overlord:config/_common:lib/*:$HADOOP_CLASS_PATH:extensions/druid-avro-extensions/*  \
  io.druid.cli.Main index hadoop \
  wikipedia_hadoop_parquet_job.json
```

Almost all the fields are required, including `inputFormat`, `metadataUpdateSpec`(`type`, `connectURI`, `user`, `password`, `segmentTable`). Set `jobProperties` to make hdfs path timezone unrelated.

```json
{
  "type": "index_hadoop",
  "spec": {
    "ioConfig": {
      "type": "hadoop",
      "inputSpec": {
        "type": "static",
        "inputFormat": "io.druid.data.input.avro.DruidAvroParquetInputFormat",
        "paths": "..."
      },
      "metadataUpdateSpec": {
        "type": "postgresql",
        "connectURI": "jdbc:postgresql://localhost/druid",
        "user" : "druid",
        "password" : "xxxx",
        "segmentTable": "druid_segments"
      },
      "segmentOutputPath": "/tmp/segments"
    },
    "dataSchema": {
      "dataSource": "wikipedia",
      "parser": {
        "type": "avro_hadoop",
        "parseSpec": {
          "format": "json",
          "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
          },
          "dimensionsSpec": {
            "dimensions": [
              "page",
              "language",
              "user",
              "unpatrolled"
            ],
            "dimensionExclusions": [],
            "spatialDimensions": []
          }
        }
      },
      "metricsSpec": [{
        "type": "count",
        "name": "count"
      }, {
        "type": "doubleSum",
        "name": "deleted",
        "fieldName": "deleted"
      }, {
        "type": "doubleSum",
        "name": "delta",
        "fieldName": "delta"
      }],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE",
        "intervals": ["2013-08-30/2013-09-02"]
      }
    },
    "tuningConfig": {
      "type": "hadoop",
      "workingPath": "tmp/working_path",
      "partitionsSpec": {
        "targetPartitionSize": 5000000
      },
      "jobProperties" : {
        "mapreduce.map.java.opts": "-server -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps",
        "mapreduce.reduce.java.opts": "-server -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps",
        "mapred.child.java.opts": "-server -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
      },
      "leaveIntermediate": true
    }
  }
}

```
