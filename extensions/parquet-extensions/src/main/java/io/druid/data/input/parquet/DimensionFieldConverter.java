package io.druid.data.input.parquet;

import com.google.api.client.util.Lists;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Convert non string dimensions to string
 * int/boolean/long => string
 * float/double dimensions will throw exceptions
 * <p/>
 * string dimensions should support dictionary decoding
 */
public class DimensionFieldConverter
{

  /**
   * convert all non-string dimension field to string
   *
   * @author du00
   */
  public static class NonStringFieldConverter extends PrimitiveConverter
  {

    private DruidGroupConverter parent;
    private String field;

    public NonStringFieldConverter(DruidGroupConverter parent, String field)
    {
      this.parent = parent;
      this.field = field;
    }

    @Override
    public void addBoolean(boolean value)
    {
      parent.addDimensionMetric(field, value + "");
    }

    @Override
    public void addInt(int value)
    {
      parent.addDimensionMetric(field, value + "");
    }

    @Override
    public void addLong(long value)
    {
      parent.addDimensionMetric(field, value + "");
    }

  }

  /**
   * enable string field read from dictionary , (refer to parquet-pig)
   *
   * @author du00
   */
  public static class StringFieldConverter extends PrimitiveConverter
  {
    private DruidGroupConverter parent;
    private String field;

    private boolean dictionarySupport;
    private String[] dict;

    public StringFieldConverter(DruidGroupConverter parent, String field)
    {
      this.parent = parent;
      this.field = field;
    }

    @Override
    final public void addBinary(Binary value)
    {
      parent.addDimensionMetric(field, value.toStringUsingUTF8());
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
      parent.addDimensionMetric(field, dict[dictionaryId]);
    }
  }

  public static class ListFieldConverter extends DruidGroupConverter
  {

    private final Converter[] converters;
    private DruidGroupConverter parent;
    private String field;

    public ListFieldConverter(DruidGroupConverter parent, GroupType schema, String name)
    {
      this.parent = parent;
      this.field = name;
      this.converters = new Converter[schema.getFieldCount()];

      int i = 0;
      for (Type field : schema.getFields()) {
        if (field.isPrimitive()) {
          if (field.getOriginalType() == OriginalType.UTF8) {
            converters[i++] = new StringFieldConverter(this, null);
          } else {
            converters[i++] = new NonStringFieldConverter(this, null);
          }
        } else {
          converters[i++] = new ListFieldConverter(this, field.asGroupType(), name);
        }
      }
    }


    private List<Object> list;

    /**
     * called at initialization based on schema
     * must consistently return the same object
     *
     * @param fieldIndex index of the field in this group
     *
     * @return the corresponding converter
     */
    @Override
    public Converter getConverter(int fieldIndex)
    {
      return converters[fieldIndex];
    }

    /**
     * called at the beginning of the group managed by this converter
     */
    @Override
    public void start()
    {
      list = Lists.newArrayList();
    }

    /**
     * call at the end of the group
     */
    @Override
    public void end()
    {
      parent.addDimensionMetric(field, list);
    }

    @Override
    public void addDimensionMetric(String field, Object value)
    {
      if (value instanceof List) {
        list.addAll((List) value);
      } else {
        list.add(value);
      }
    }

    @Override
    public void setTimestamp(DateTime dateTime)
    {

    }
  }

}
