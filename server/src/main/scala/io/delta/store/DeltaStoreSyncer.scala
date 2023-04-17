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

package io.delta.store

import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer

object DeltaStoreSyncer {
  private val parser = {
    val parser = ArgumentParsers
      .newFor("Delta Store Syncer")
      .build()
      .defaultHelp(true)
      .description("Start the Delta Store Syncer.")
    parser
      .addArgument("-c", "--configfile")
      .required(true)
      .metavar("CONFIGFILE")
      .dest("configfile")
      .help("The syncer config file")
    parser
      .addArgument("-d", "--daemon")
      .dest("daemon")
      .action(Arguments.storeTrue())
      .help("The daemon mode")
    parser
  }

  def main(args: Array[String]): Unit = {
    val ns = parser.parseArgsOrFail(args)
    val configFile = ns.getString("configfile")
    val storeConfig = DeltaStoreConfig
      .load(configFile)
    DeltaStoreConfigManager.use(storeConfig)
    DeltaStoreTableManager.use(storeConfig)
    val manager = DeltaStoreTableManager()
    if (ns.getBoolean("daemon")) {
      manager.ready()
    } else {
      manager.register()
      manager.update()
    }
  }
}
