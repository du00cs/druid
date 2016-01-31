/**
 * Copyright (c) 2014 XiaoMi Inc. All Rights Reserved.
 * Authors: du00 <duninglin@xiaomi.com>
 */

package io.druid.data.input.parquet;
import io.druid.indexer.DeterminePartitionsJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class ParquetInputRowJobTest {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
				"example/wikipedia_hadoop_config.json"));
		Job job = Job.getInstance(conf, "parquet test");
		System.out.println(config.getParser());

		config.intoConfiguration(job);

		job.setJarByClass(ParquetInputRowJobTest.class);

		job.setMapperClass(DeterminePartitionsJob.DeterminePartitionsGroupByMapper.class);
		job.setInputFormatClass(DruidParquetInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("example/wikipedia.gz.parquet"));

		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.waitForCompletion(true);

		FileSystem fs = FileSystem.getLocal(conf);
		fs.deleteOnExit(new Path("output"));
		fs.close();
	}
}
