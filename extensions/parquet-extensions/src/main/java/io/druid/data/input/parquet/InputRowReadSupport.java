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

  private MessageType getPartialReadSchame(InitContext context)
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
    // String partialSchemaString = context.getConfiguration().get(ReadSupport.PARQUET_READ_SCHEMA);
    MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), getPartialReadSchame(context));
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
