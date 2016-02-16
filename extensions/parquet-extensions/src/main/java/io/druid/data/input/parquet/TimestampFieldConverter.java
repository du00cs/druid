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
import org.apache.parquet.io.api.PrimitiveConverter;
import org.joda.time.DateTime;

import java.util.List;

/**
 * timestamp field converter, only accept types of String and Long
 */
public class TimestampFieldConverter extends PrimitiveConverter
{
  private final DruidGroupConverter parent;
  private boolean dictionarySupport;
  private List<String> dict;

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
    dict = Lists.newArrayListWithCapacity(dictionary.getMaxId() + 1);
    for (int i = 0; i <= dictionary.getMaxId(); i++) {
      dict.add(i, dictionary.decodeToBinary(i).toStringUsingUTF8());
    }
    dictionarySupport = true;
  }

  @Override
  public void addValueFromDictionary(int dictionaryId)
  {
    parent.setTimestamp(new DateTime(dict.get(dictionaryId)));
  }

  @Override
  public void addLong(long value)
  {
    parent.setTimestamp(new DateTime(value));
  }
}
