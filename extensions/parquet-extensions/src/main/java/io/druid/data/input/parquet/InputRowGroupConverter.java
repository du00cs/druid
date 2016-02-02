package io.druid.data.input.parquet;

import com.google.api.client.util.Maps;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class InputRowGroupConverter extends DruidGroupConverter
{
  private Converter[] converters;

  private final InputRowGroupConverter parent;

  protected DateTime datetime;
  protected Map<String, Object> event;
  private List<String> dimensions;

  public InputRowGroupConverter(
      InputRowGroupConverter parent, MessageType schema, String timestamp,
      List<String> dimensions
  )
  {
    this.dimensions = dimensions;
    this.parent = parent;
    converters = new Converter[schema.getFieldCount()];

    for (int i = 0; i < converters.length; i++) {
      Type type = schema.getType(i);
      if (type.isPrimitive()) {
        if (type.getName().equals(timestamp)) {
          converters[i] = new TimestampFieldConverter(this);
        } else if (dimensions.contains(type.getName())) {
          switch (type.getOriginalType()) {
            case UTF8:
              converters[i] = new DimensionFieldConverter.StringFieldConverter(this, type.getName());
              break;
            default:
              converters[i] = new DimensionFieldConverter.NonStringFieldConverter(this, type.getName());
              break;
          }
        } else {
          converters[i] = new MetricFieldConverter(this, type.getName());
        }
      } else if (type.getOriginalType() == OriginalType.LIST) {
        converters[i] = new DimensionFieldConverter.ListFieldConverter(this, type.asGroupType(), type.getName());
      } else {
        throw new IllegalArgumentException("Incompatibal field: <"
                                           + type.getName()
                                           + ", "
                                           + type.getOriginalType()
                                           + ">");
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

  @Override
  public void start()
  {
    event = Maps.newHashMap();
  }

  @Override
  public void end()
  {
  }

  @Override
  public void addDimensionMetric(String field, Object value)
  {
    event.put(field, value);
  }

  @Override
  public void setTimestamp(DateTime dateTime)
  {
    this.datetime = dateTime;
  }
}
