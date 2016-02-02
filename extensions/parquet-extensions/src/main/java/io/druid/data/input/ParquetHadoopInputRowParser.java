package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;

public class ParquetHadoopInputRowParser implements InputRowParser<InputRow>
{

  private final ParseSpec parseSpec;

  @JsonCreator
  public ParquetHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
  }

  @Override
  public InputRow parse(InputRow input)
  {
    return input;
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ParquetHadoopInputRowParser(parseSpec);
  }
}
