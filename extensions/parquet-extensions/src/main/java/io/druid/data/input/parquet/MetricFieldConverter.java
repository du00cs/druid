package io.druid.data.input.parquet;

import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * metric field converter, only accept double, float and int/long
 */
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
    parent.addDimensionMetric(field, value);
  }

  @Override
  public void addFloat(float value)
  {
    parent.addDimensionMetric(field, value);
  }

  @Override
  public void addInt(int value)
  {
    parent.addDimensionMetric(field, value);
  }

  @Override
  public void addLong(long value)
  {
    parent.addDimensionMetric(field, value);
  }
}
