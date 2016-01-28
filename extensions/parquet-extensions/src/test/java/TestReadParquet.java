import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.Log;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestReadParquet extends Configured implements Tool
{
  private static final Log LOG = Log.getLog(TestReadParquet.class);

  private static class FieldDescription
  {
    public String constraint;
    public String type;
    public String name;
  }

  private static class RecordSchema
  {
    public RecordSchema(String message)
    {
      fields = new ArrayList<FieldDescription>();
      List<String> elements = Arrays.asList(message.split("\n"));
      Iterator<String> it = elements.iterator();
      while (it.hasNext()) {
        String line = it.next().trim().replace(";", "");
        ;
        System.err.println("RecordSchema read line: " + line);
        if (line.startsWith("optional") || line.startsWith("required")) {
          String[] parts = line.split(" ");
          FieldDescription field = new FieldDescription();
          field.constraint = parts[0];
          field.type = parts[1];
          field.name = parts[2];
          fields.add(field);
        }
      }
    }

    private List<FieldDescription> fields;

    public List<FieldDescription> getFields()
    {
      return fields;
    }
  }

  /*
   * Read a Parquet record, write a CSV record
   */
  public static class ReadRequestMap extends Mapper<LongWritable, SimpleGroup, NullWritable, Text>
  {
    private static List<FieldDescription> expectedFields = null;

    @Override
    public void map(LongWritable key, SimpleGroup value, Context context) throws IOException, InterruptedException
    {
      NullWritable outKey = NullWritable.get();

      String line = value.toString();

      StringBuilder csv = new StringBuilder();
      for (int i = 0; i < value.getType().getFieldCount(); i++) {
        csv.append(value.getValueToString(i, 0) + ",");
      }

      System.out.println(csv.toString().substring(0, csv.length() -1));

//      context.write(outKey, new Text(csv.toString()));
    }
  }

  public int run(String[] args) throws Exception
  {
    getConf().set("mapred.textoutputformat.separator", ",");

    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(ReadRequestMap.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(ExampleInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    try {
      int res = ToolRunner.run(new Configuration(), new TestReadParquet(), args);
      System.exit(res);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
    }
  }
}
