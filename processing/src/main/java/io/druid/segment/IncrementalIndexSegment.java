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

package io.druid.segment;

import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class IncrementalIndexSegment implements Segment
{
  private final IncrementalIndex index;
  private final String segmentIdentifier;

  public IncrementalIndexSegment(
      IncrementalIndex index,
      String segmentIdentifier
  )
  {
    this.index = index;
    this.segmentIdentifier = segmentIdentifier;
  }

  @Override
  public String getIdentifier()
  {
    return segmentIdentifier;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return new IncrementalIndexStorageAdapter(index);
  }

  @Override
  public void close() throws IOException
  {
    index.close();
  }
}
