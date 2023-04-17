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

package io.delta.store.helpers

import scala.reflect.ClassTag

import io.delta.store.internal.data.CloseableIterator

object Implicits {
  implicit class CloseableIteratorOps[T: ClassTag](
      private val iter: CloseableIterator[T]
  ) {
    import scala.collection.JavaConverters._

    /* Convert the [[CloseableIterator]] (Java) to an in-memory [[Array]]
     * (Scala).
     *
     * [[scala.collection.Iterator.toArray]] is used over
     * [[scala.collection.Iterable.toSeq]] because `toSeq` is lazy, meaning
     * `iter.close()` would be called before the Seq was actually generated.
     */
    def toArray: Array[T] = {
      try {
        iter.asScala.toArray
      } finally {
        iter.close()
      }
    }
  }
  implicit class DeltaStorageCloseableIteratorOps[T: ClassTag](
      private val iter: io.delta.storage.CloseableIterator[T]
  ) {
    import scala.collection.JavaConverters._

    /* Convert the [[io.delta.storage.CloseableIterator]] (Java) to an
     * in-memory [[Array]] (Scala).
     *
     * [[scala.collection.Iterator.toArray]] is used over
     * [[scala.collection.Iterable.toSeq]] because `toSeq` is lazy, meaning
     * `iter.close()` would be called before the Seq was actually generated.
     */
    def toArray: Array[T] = {
      try {
        iter.asScala.toArray
      } finally {
        iter.close()
      }
    }
  }
}
