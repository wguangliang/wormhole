/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.wormhole.sparkx.batchflow

import edp.wormhole.common._
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sparkx.common.SparkContextUtils.createKafkaStream
import edp.wormhole.sparkx.common.{KafkaInputConfig, SparkContextUtils, SparkUtils, WormholeConfig}
import edp.wormhole.sparkx.directive.DirectiveFlowWatch
import edp.wormhole.sparkx.memorystorage.OffsetPersistenceManager
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkx.udf.UdfWatch
import edp.wormhole.util.JsonUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BatchflowStarter extends App with EdpLogging {
  SparkContextUtils.setLoggerLevel()

  // 传入的参数为能转换为WormholeConfig类型的字符串
  logInfo("swiftsConfig:" + args(0))
  val config: WormholeConfig = JsonUtils.json2caseClass[WormholeConfig](args(0))
  val appId = SparkUtils.getAppId  // spark.yarn.app.id 或者 工程名
  // 创建 feedback kafka
  WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config)

  val sparkConf = new SparkConf()
    .setMaster(config.spark_config.master)
    .set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS")
    .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    .set("spark.sql.shuffle.partitions", config.spark_config.`spark.sql.shuffle.partitions`.toString)
    .set(if (SparkUtils.isLocalMode(config.spark_config.master)) "spark.sql.warehouse.dir" else "",
      if (SparkUtils.isLocalMode(config.spark_config.master)) "file:///" else "")
    .setAppName(config.spark_config.stream_name)
  val sparkContext = new SparkContext(sparkConf)
  val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(config.kafka_input.batch_duration_seconds))

  /**
    * 初始化udf，并zk监控
    * 创建zk目录：/wormhole/${stream_id}/udf，并监听该目录下的子节点
    */
  UdfWatch.initUdf(config, appId,session)

//  if (config.udf.isDefined) {
//    import collection.JavaConversions._
//    new UdfRegister().udfRegister(config.udf.get, session.sqlContext)
//  }
  /**
    * 初始化flow
    */
  DirectiveFlowWatch.initFlow(config, appId)

  val kafkaInput: KafkaInputConfig = OffsetPersistenceManager.initOffset(config, appId)
  val kafkaStream = createKafkaStream(ssc, kafkaInput)

  // 数据主要处理逻辑
  BatchflowMainProcess.process(kafkaStream, config, session)

  // 记录启动的appId
  SparkContextUtils.checkSparkRestart(config.zookeeper_path, config.spark_config.stream_id, appId)
  SparkContextUtils.deleteZookeeperOldAppidPath(appId, config.zookeeper_path, config.spark_config.stream_id)
  WormholeZkClient.createPath(config.zookeeper_path, WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + "/" + appId)

  logInfo("all init finish,to start spark streaming")
  SparkContextUtils.startSparkStreaming(ssc)


}
