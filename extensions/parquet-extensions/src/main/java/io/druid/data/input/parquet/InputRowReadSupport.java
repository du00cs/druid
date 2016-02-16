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
import com.google.api.client.util.Sets;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * `copy` from parquet-hadoop examples
 *
 * read only "ts + dimensions + metric"
 */
public class InputRowReadSupport extends ReadSupport<InputRow>
{

  private MessageType getPartialReadSchema(InitContext context)
  {
    MessageType fullSchema = context.getFileSchema();

    String name = fullSchema.getName();

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    String tsField = config.getParser().getParseSpec().getTimestampSpec().getTimestampColumn();
    List<String> dimensions = config.getParser().getParseSpec().getDimensionsSpec().getDimensions();
    Set<String> metricsFields = Sets.newHashSet();
    for (AggregatorFactory agg : config.getSchema().getDataSchema().getAggregators()) {
      if (!agg.getName().equals("count")) {
        metricsFields.add(agg.getName());
      }
    }

    List<Type> partialFields = Lists.newArrayList();

    for (Type type : fullSchema.getFields()) {
      if (tsField.equals(type.getName())
          || dimensions.contains(type.getName())
          || metricsFields.contains(type.getName())) {
        partialFields.add(type);
      }
    }

    return new MessageType(name, partialFields);
  }

  public ReadContext init(InitContext context)
  {
    MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), getPartialReadSchema(context));
    return new ReadContext(requestedProjection);
  }

  @Override
  public RecordMaterializer<InputRow> prepareForRead(
      Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, ReadContext readContext
  )
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(configuration);
    String timestamp = config.getParser().getParseSpec().getTimestampSpec().getTimestampColumn();
    List<String> dimensions = config.getParser().getParseSpec().getDimensionsSpec().getDimensions();

    return new InputRowConverter(readContext.getRequestedSchema(), timestamp, dimensions);
  }

}
