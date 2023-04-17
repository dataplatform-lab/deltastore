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

package io.delta.store.sources

/* [[org.apache.hadoop.conf.Configuration]] entries for Delta Standalone
 * features.
 */
object StandaloneHadoopConf {
  /* Time zone as which time-based parquet values will be encoded and decoded.
   */
  val PARQUET_DATA_TIME_ZONE_ID =
    "io.delta.store.PARQUET_DATA_TIME_ZONE_ID"

  /* Legacy key for the class name of the desired [[LogStore]] implementation
   * to be used.
   */
  val LEGACY_LOG_STORE_CLASS_KEY = "io.delta.store.LOG_STORE_CLASS_KEY"

  /* Key for the class name of the desired [[LogStore]] implementation to be
   * used.
   */
  val LOG_STORE_CLASS_KEY = "delta.logStore.class"

  /* If enabled, partition values evaluation result will be cached in partition
   * pruning in `FilteredDeltaScanImpl::accept`. By default, this feature is
   * enabled. Set to `false` to disable.
   */
  val PARTITION_FILTER_RECORD_CACHING_KEY =
    "io.delta.store.partitionFilterRecordCaching.enabled"
}
