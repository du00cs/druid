package io.druid.data.input.parquet;

import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

/**
 * `copy` from parquet-hadoop examples
 */
public class InputRowReadSupport extends ReadSupport<InputRow>
{

  public ReadContext init(InitContext context) {
    String partialSchemaString = context.getConfiguration().get(ReadSupport.PARQUET_READ_SCHEMA);
    MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), partialSchemaString);
    return new ReadContext(requestedProjection);
  }

	@Override
	public RecordMaterializer<InputRow> prepareForRead(
	    Configuration configuration, Map<String, String> keyValueMetaData,
	    MessageType fileSchema, ReadContext readContext)
	{
		HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(configuration);
		String timestamp = config.getParser().getParseSpec().getTimestampSpec().getTimestampColumn();
		List<String> dimensions = config.getParser().getParseSpec().getDimensionsSpec().getDimensions();

		return new InputRowConverter(readContext.getRequestedSchema(), timestamp, dimensions);
	}

}
