package io.druid.data.input.parquet;

import io.druid.data.input.MapBasedInputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class DruidParquetInputFormatTest
{

  /**
   * test file `example/wikipedia.gz.parquet` contains the same content as `examples/indexing/wikipedia_data.json`
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void fullSchemaTest() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/wikipedia_hadoop_parquet.json"));

    config.intoConfiguration(job);

    File testFile = new File("example/wikipedia.gz.parquet");
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(DruidParquetInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    int nRecords = 0;
    if (reader.nextKeyValue()) {
      nRecords++;
    }

    MapBasedInputRow inputRow = (MapBasedInputRow) reader.getCurrentValue();
    System.out.println(inputRow);

    // {"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}

    assertEquals(inputRow.getTimestamp(), new DateTime("2013-08-31T01:02:33Z"));
    assertEquals(inputRow.getDimension("language").get(0), "en");
    assertEquals(inputRow.getLongMetric("delta"), -143);

    while (reader.nextKeyValue()) {
      nRecords++;
    }
    assertEquals(nRecords, 5);
  }

  @Test
  public void partialReadTest() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/wikipedia_hadoop_paruqet_partial.json"));

    config.intoConfiguration(job);

    File testFile = new File("example/wikipedia.gz.parquet");
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(DruidParquetInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    reader.nextKeyValue();

    MapBasedInputRow inputRow = (MapBasedInputRow) reader.getCurrentValue();
    System.out.println(inputRow);

    assertFalse(inputRow.getEvent().containsKey("anonymous"));
    assertFalse(inputRow.getEvent().containsKey("added"));
  }

  @Test
  public void listTypeDimensionTest() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/wikipedia_hadoop_paruqet_partial.json"));

    config.intoConfiguration(job);

    File testFile = new File("example/wikipedia_list.parquet");
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(DruidParquetInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    reader.nextKeyValue();

    String[] expected = new String[]{"en", "zh"};

    MapBasedInputRow inputRow = (MapBasedInputRow) reader.getCurrentValue();
    assertArrayEquals(inputRow.getDimension("language").toArray(), expected);
  }
}
