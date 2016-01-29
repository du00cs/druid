package io.druid.data.input.parquet;

import org.apache.parquet.hadoop.ParquetInputFormat;

import io.druid.data.input.InputRow;

public class ParquetValueInputFormat extends ParquetInputFormat<InputRow>
{
	public ParquetValueInputFormat(){
		super(InputRowReadSupport.class);
	}
}
