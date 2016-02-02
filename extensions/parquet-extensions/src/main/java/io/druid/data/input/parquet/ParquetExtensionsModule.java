package io.druid.data.input.parquet;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.data.input.ParquetHadoopInputRowParser;
import io.druid.initialization.DruidModule;

import java.util.List;

public class ParquetExtensionsModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("ParquetInputRowParserModule")
            .registerSubtypes(
                new NamedType(ParquetHadoopInputRowParser.class, "parquet")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
