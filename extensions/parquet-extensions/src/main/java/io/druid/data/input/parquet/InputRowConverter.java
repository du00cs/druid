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

import io.druid.data.input.InputRow;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.List;

public class InputRowConverter extends RecordMaterializer<InputRow>
{

	private final InputRowGroupConverter root;

	public InputRowConverter(MessageType schema, String timestamp, List<String> dimensions)
	{
		this.root = new InputRowGroupConverter(null, schema, timestamp, dimensions);
	}

	@Override
	public InputRow getCurrentRecord()
	{
		return root.getCurrent();
	}

	@Override
	public GroupConverter getRootConverter()
	{
		return root;
	}

}
