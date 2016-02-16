/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
   */
  public static class NonStringFieldConverter extends PrimitiveConverter
  {

    private final DruidGroupConverter parent;
    private final String field;

    public NonStringFieldConverter(DruidGroupConverter parent, String field)
    {
      this.parent = parent;
      this.field = field;
    }

    @Override
    public void addBoolean(boolean value)
    {
      parent.addDimensionMetric(field, Boolean.toString(value));
    }

    @Override
    public void addInt(int value)
    {
      parent.addDimensionMetric(field, Integer.toString(value));
    }

    @Override
    public void addLong(long value)
    {
      parent.addDimensionMetric(field, Long.toString(value));
    }

  }

  /**
   * enable string field read from dictionary , (refer to parquet-pig)
   */
  public static class StringFieldConverter extends PrimitiveConverter
  {
    private final DruidGroupConverter parent;
    private final String field;

    private boolean dictionarySupport;
    private List<String> dict;

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
      dict = Lists.newArrayListWithCapacity(dictionary.getMaxId() + 1);
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        dict.set(i, dictionary.decodeToBinary(i).toStringUsingUTF8());
      }
      dictionarySupport = true;
    }

    @Override
    public void addValueFromDictionary(int dictionaryId)
    {
      parent.addDimensionMetric(field, dict.get(dictionaryId));
    }
  }

  public static class ListFieldConverter extends DruidGroupConverter
  {

    private final List<Converter> converters;
    private final DruidGroupConverter parent;
    private final String field;

    private List<Object> list;

    public ListFieldConverter(DruidGroupConverter parent, GroupType schema, String name)
    {
      this.parent = parent;
      this.field = name;
      this.converters = Lists.newArrayListWithCapacity(schema.getFieldCount());

      for (Type field : schema.getFields()) {
        if (field.isPrimitive()) {
          if (field.getOriginalType() == OriginalType.UTF8) {
            converters.add(new StringFieldConverter(this, null));
          } else {
            converters.add(new NonStringFieldConverter(this, null));
          }
        } else {
          converters.add(new ListFieldConverter(this, field.asGroupType(), name));
        }
      }
    }

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
      return converters.get(fieldIndex);
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

    /**
     * leave empty, timestamp should not be a list
     */
    @Override
    public void setTimestamp(DateTime dateTime)
    {

    }
  }

}
