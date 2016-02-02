package io.druid.data.input.parquet;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.joda.time.DateTime;

/**
 * timestamp field converter, only accept types of String and Long
 */
public class TimestampFieldConverter extends PrimitiveConverter
{
  private DruidGroupConverter parent;
  private boolean dictionarySupport;
  private String[] dict;

  public TimestampFieldConverter(DruidGroupConverter parent)
  {
    this.parent = parent;
  }

  @Override
  final public void addBinary(Binary value)
  {
    parent.setTimestamp(new DateTime(value.toStringUsingUTF8()));
  }

  @Override
  public boolean hasDictionarySupport()
  {
    return dictionarySupport;
  }

  @Override
  public void setDictionary(Dictionary dictionary)
  {
    dict = new String[dictionary.getMaxId() + 1];
    for (int i = 0; i <= dictionary.getMaxId(); i++) {
      dict[i] = dictionary.decodeToBinary(i).toStringUsingUTF8();
    }
    dictionarySupport = true;
  }

  @Override
  public void addValueFromDictionary(int dictionaryId)
  {
    parent.setTimestamp(new DateTime(dict[dictionaryId]));
  }

  @Override
  public void addLong(long value)
  {
    parent.setTimestamp(new DateTime(value));
  }
}
