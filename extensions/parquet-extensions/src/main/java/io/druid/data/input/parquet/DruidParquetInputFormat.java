package io.druid.data.input.parquet;

import org.apache.parquet.hadoop.ParquetInputFormat;

import io.druid.data.input.InputRow;

/**
 * The implementation refers a lot to parquet-hadoop `ExampleInputFormat`
 */
public class DruidParquetInputFormat extends ParquetInputFormat<InputRow>
{
	public DruidParquetInputFormat(){
		super(InputRowReadSupport.class);
	}
}
