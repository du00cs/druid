package io.druid.data.input.parquet;

import org.apache.parquet.io.api.GroupConverter;
import org.joda.time.DateTime;

abstract class DruidGroupConverter extends GroupConverter
{
  public abstract void addDimensionMetric(String field, Object value);

  public abstract void setTimestamp(DateTime dateTime);
}
