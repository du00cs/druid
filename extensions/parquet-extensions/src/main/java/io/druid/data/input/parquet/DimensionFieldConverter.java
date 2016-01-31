package io.druid.data.input.parquet;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Convert non string dimensions to string
 *  int/boolean/long => string
 *  float/double dimensions will throw exceptions
 *
 *  string dimensions should support dictionary decoding
 *
 */
public class DimensionFieldConverter
{

	/**
	 * convert all non-string dimension field to string
	 * 
	 * @author du00
	 *
	 */
	public static class NonStringFieldConverter extends PrimitiveConverter
	{

		private InputRowGroupConverter parent;
		private String field;

		public NonStringFieldConverter(InputRowGroupConverter parent, String field)
		{
			this.parent = parent;
			this.field = field;
		}

		@Override
		public void addBoolean(boolean value)
		{
			parent.addToEvent(field, value + "");
		}

		@Override
		public void addInt(int value)
		{
			parent.addToEvent(field, value + "");
		}

		@Override
		public void addLong(long value)
		{
			parent.addToEvent(field, value + "");
		}

	}

	/**
	 * enable string field read from dictionary , (refer to parquet-pig)
	 * 
	 * @author du00
	 *
	 */
	public static class StringFieldConverter extends PrimitiveConverter
	{
		private InputRowGroupConverter parent;
		private String field;

		private boolean dictionarySupport;
		private String[] dict;

		public StringFieldConverter(InputRowGroupConverter parent, String field)
		{
			this.parent = parent;
			this.field = field;
		}

		@Override
		final public void addBinary(Binary value)
		{
			parent.addToEvent(field, value.toStringUsingUTF8());
		}

		@Override
		public boolean hasDictionarySupport()
		{
			return dictionarySupport;
		}

		@Override
		public void setDictionary(Dictionary dictionary)
		{
			dict = new String[dictionary.getMaxId() + 1];
			for (int i = 0; i <= dictionary.getMaxId(); i++)
			{
				dict[i] = dictionary.decodeToBinary(i).toStringUsingUTF8();
			}
		}

		@Override
		public void addValueFromDictionary(int dictionaryId)
		{
			parent.addToEvent(field, dict[dictionaryId]);
		}
	}

}
