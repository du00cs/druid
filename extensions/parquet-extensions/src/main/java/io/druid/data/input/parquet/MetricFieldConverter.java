package io.druid.data.input.parquet;

import org.apache.parquet.io.api.PrimitiveConverter;

public class MetricFieldConverter extends PrimitiveConverter
{
	private InputRowGroupConverter parent;
	private String field;

	public MetricFieldConverter(InputRowGroupConverter parent, String field)
	{
		this.parent = parent;
		this.field = field;
	}

	@Override
	public void addDouble(double value)
	{
		parent.addToEvent(field, value);
	}

	@Override
	public void addFloat(float value)
	{
		parent.addToEvent(field, value);
	}

	@Override
	public void addInt(int value)
	{
		parent.addToEvent(field, value);
	}

	@Override
	public void addLong(long value)
	{
		parent.addToEvent(field, value);
	}
}
