/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
 * Authors: du00 <duninglin@xiaomi.com>
 */

package io.druid.data.input.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.DruidAvroReadSupport;
import org.apache.parquet.hadoop.ParquetInputFormat;

public class DruidAvroParquetInputFormat extends ParquetInputFormat<GenericRecord>
{
  public DruidAvroParquetInputFormat()
  {
    super(DruidAvroReadSupport.class);
  }
}
