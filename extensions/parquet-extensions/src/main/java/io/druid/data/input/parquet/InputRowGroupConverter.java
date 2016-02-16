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
import com.google.api.client.util.Maps;
import com.metamx.common.IAE;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class InputRowGroupConverter extends DruidGroupConverter
{
  private final List<Converter> converters;

  protected final InputRowGroupConverter parent;
  private final List<String> dimensions;

  protected DateTime datetime;
  protected Map<String, Object> event;

  public InputRowGroupConverter(
      InputRowGroupConverter parent, MessageType schema, String timestamp,
      List<String> dimensions
  )
  {
    this.dimensions = dimensions;
    this.parent = parent;
    this.converters = Lists.newArrayListWithCapacity(schema.getFieldCount());

    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type type = schema.getType(i);
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = type.asPrimitiveType();
        if (primitiveType.getName().equals(timestamp)) {
          converters.add(new TimestampFieldConverter(this));
        } else if (dimensions.contains(primitiveType.getName())) {
          // original type sometimes missing
          switch (primitiveType.getPrimitiveTypeName()) {
            case BINARY:
              converters.add(new DimensionFieldConverter.StringFieldConverter(this, type.getName()));
              break;
            default:
              converters.add(new DimensionFieldConverter.NonStringFieldConverter(this, type.getName()));
              break;
          }
        } else {
          converters.add(new MetricFieldConverter(this, type.getName()));
        }
      } else if (type.getOriginalType() == OriginalType.LIST) {
        converters.add(new DimensionFieldConverter.ListFieldConverter(this, type.asGroupType(), type.getName()));
      } else {
        throw new IAE("Incompatible field: <%s, %s>", type.getName(), type.getOriginalType());
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex)
  {
    return converters.get(fieldIndex);
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
