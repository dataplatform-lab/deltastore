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

package io.delta.store.internal.exceptions;

/*
 * Thrown when a query fails, usually because the query itself is invalid.
 */
public class DeltaStandaloneException extends RuntimeException {
	public DeltaStandaloneException() {
		super();
	}

	public DeltaStandaloneException(String message) {
		super(message);
	}

	public DeltaStandaloneException(String message, Throwable cause) {
		super(message, cause);
	}
}
