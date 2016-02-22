/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
 * Authors: du00 <duninglin@xiaomi.com>
 */

package org.apache.parquet.avro;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.avro.AvroDataSupplier;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroRecordMaterializer;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.SpecificDataSupplier;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by du00 on 16/2/22.
 */
public class DruidAvroReadSupport extends AvroReadSupport<GenericRecord>
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
          || metricsFields.contains(type.getName())
          || dimensions.size() > 0 && dimensions.contains(type.getName())
          || dimensions.size() == 0) {
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
  public RecordMaterializer<GenericRecord> prepareForRead(
      Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, ReadContext readContext
  )
  {

    MessageType parquetSchema = readContext.getRequestedSchema();
    Schema avroSchema = new AvroSchemaConverter(configuration).convert(parquetSchema);

    Class<? extends AvroDataSupplier> suppClass = configuration.getClass(
        AVRO_DATA_SUPPLIER,
        SpecificDataSupplier.class,
        AvroDataSupplier.class
    );
    AvroDataSupplier supplier = ReflectionUtils.newInstance(suppClass, configuration);
    return new AvroRecordMaterializer<GenericRecord>(parquetSchema, avroSchema, supplier.get());
  }

}
