/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
	    @JsonProperty("parseSpec") ParseSpec parseSpec)
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
