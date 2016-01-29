package io.druid.data.input.parquet;

import java.util.List;
import java.util.Map;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;

import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;

public class InputRowGroupConverter extends GroupConverter
{
	private Converter[] converters;

	private final InputRowGroupConverter parent;

	protected DateTime datetime;
	protected Map<String, Object> event;
	private List<String> dimensions;

	public InputRowGroupConverter(InputRowGroupConverter parent, MessageType schema, String timestamp,
	    List<String> dimensions)
	{
		this.dimensions = dimensions;
		this.parent = parent;
		converters = new Converter[schema.getFieldCount()];

		for (int i = 0; i < converters.length; i++)
		{
			Type type = schema.getType(i);
			if (type.isPrimitive())
			{
				if (type.getName().equals(timestamp))
				{
					converters[i] = new TimestampFieldConverter(parent);
				} else if (dimensions.contains(type.getName()))
				{
					switch (type.getOriginalType())
					{
					case UTF8:
						converters[i] = new DimensionFieldConverter.StringFieldConverter(parent, type.getName());
						break;
					default:
						converters[i] = new DimensionFieldConverter.NonStringFieldConverter(parent, type.getName());
						break;
					}
				} else
				{
					converters[i] = new MetricFieldConverter(parent, type.getName());
				}
			} else
			{
				throw new IllegalArgumentException("Incompatibal type: " + type.getName());
			}
		}
	}

	@Override
	public Converter getConverter(int fieldIndex)
	{
		return converters[fieldIndex];
	}

	public InputRow getCurrent()
	{
		return new MapBasedInputRow(datetime, dimensions, event);
	}

	public void setTimestamp(DateTime datetime)
	{
		this.datetime = datetime;
	}

	public void addToEvent(String field, Object value)
	{
		event.put(field, value);
	}

	@Override
	public void start()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void end()
	{
		// TODO Auto-generated method stub

	}
}
