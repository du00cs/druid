/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
 * Authors: du00 <duninglin@xiaomi.com>
 */

package io.druid.data.input;

import io.druid.data.input.avro.DruidAvroParquetInputFormat;
import io.druid.data.input.avro.GenericRecordAsMap;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

public class DruidAvroParquetInputFormatTest
{
  @Test
  public void test() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/wikipedia_hadoop_parquet.json"));

    config.intoConfiguration(job);

    File testFile = new File("example/wikipedia_list.parquet");
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(DruidAvroParquetInputFormat.class, job.getConfiguration());

    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    reader.nextKeyValue();

    GenericRecord data = (GenericRecord) reader.getCurrentValue();

    GenericRecordAsMap record = new GenericRecordAsMap(data, false);

    System.out.println(record);

    assertEquals(data.get("added"), 57L);

    System.out.println(data.get("language"));
    reader.close();
  }
}
