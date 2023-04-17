/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.store.internal.types;

import java.util.Locale;
import java.util.Objects;

import io.delta.store.helpers.DataTypeParser;

/*
 * The base type of all {@code io.delta.store} data types. Represents a
 * bare-bones Java implementation of the Spark SQL <a href=
 * "https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DataType.scala"
 * target="_blank">DataType</a>, allowing Spark SQL schemas to be represented in
 * Java.
 */
public abstract class DataType {

	/*
	 * Parses the input {@code json} into a {@link DataType}.
	 *
	 * @param json the {@link String} json to parse
	 * 
	 * @return the parsed {@link DataType}
	 */
	public static DataType fromJson(String json) {
		return DataTypeParser.fromJson(json);
	}

	/*
	 * @return the name of the type used in JSON serialization
	 */
	public String getTypeName() {
		String tmp = this.getClass().getSimpleName();
		tmp = stripSuffix(tmp, "$");
		tmp = stripSuffix(tmp, "Type");
		tmp = stripSuffix(tmp, "UDT");
		return tmp.toLowerCase(Locale.ROOT);
	}

	/*
	 * @return a readable {@code String} representation for the type
	 */
	public String getSimpleString() {
		return getTypeName();
	}

	/*
	 * @return a {@code String} representation for the type saved in external
	 * catalogs
	 */
	public String getCatalogString() {
		return getSimpleString();
	}

	/*
	 * @return a JSON {@code String} representation of the type
	 */
	public String toJson() {
		return DataTypeParser.toJson(this);
	}

	/*
	 * @return a pretty (i.e. indented) JSON {@code String} representation of the
	 * type
	 */
	public String toPrettyJson() {
		return DataTypeParser.toPrettyJson(this);
	}

	/*
	 * Builds a readable {@code String} representation of the {@code ArrayType}
	 */
	protected static void buildFormattedString(DataType dataType, String prefix, StringBuilder builder) {
		if (dataType instanceof ArrayType) {
			((ArrayType) dataType).buildFormattedString(prefix, builder);
		}
		if (dataType instanceof StructType) {
			((StructType) dataType).buildFormattedString(prefix, builder);
		}
		if (dataType instanceof MapType) {
			((MapType) dataType).buildFormattedString(prefix, builder);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		DataType that = (DataType) o;
		return getTypeName().equals(that.getTypeName());
	}

	public boolean equivalent(DataType dt) {
		return this.equals(dt);
	}

	@Override
	public int hashCode() {
		return Objects.hash(getTypeName());
	}

	private String stripSuffix(String orig, String suffix) {
		if (null != orig && orig.endsWith(suffix)) {
			return orig.substring(0, orig.length() - suffix.length());
		}
		return orig;
	}
}
