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

import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * metric field converter, only accept double, float and int/long
 */
public class MetricFieldConverter extends PrimitiveConverter
{
  private final InputRowGroupConverter parent;
  private final String field;

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
