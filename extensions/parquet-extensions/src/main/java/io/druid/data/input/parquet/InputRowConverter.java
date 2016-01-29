package io.druid.data.input.parquet;

import java.util.List;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import io.druid.data.input.InputRow;

public class InputRowConverter extends RecordMaterializer<InputRow>
{

	private InputRowGroupConverter root;

	public InputRowConverter(MessageType schema, String timestamp, List<String> dimensions)
	{
		this.root = new InputRowGroupConverter(null, schema, timestamp, dimensions);
	}

	@Override
	public InputRow getCurrentRecord()
	{
		return root.getCurrent();
	}

	@Override
	public GroupConverter getRootConverter()
	{
		return root;
	}

}
