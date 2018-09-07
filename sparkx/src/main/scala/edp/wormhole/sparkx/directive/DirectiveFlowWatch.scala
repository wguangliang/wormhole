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


package edp.wormhole.sparkx.directive

import edp.wormhole.common.{StreamType, WormholeConstants}
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sparkx.batchflow.BatchflowDirective
import edp.wormhole.sparkx.common.WormholeConfig
import edp.wormhole.sparkx.hdfslog.{HdfsDirective, HdfsMainProcess}
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.router.{RouterDirective, RouterMainProcess}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.{UmsProtocolType, UmsSchemaUtils}

object DirectiveFlowWatch extends EdpLogging {

  val flowRelativePath = "/flow"

  /**
    * 初始化 flow
    * 1)创建zk目录 /wormhole/${stream_id}/flow
    * 2)监控该目录的子节点
    * 3)配置信息注册
    *
    * @param config
    * @param appId
    */
  def initFlow(config: WormholeConfig, appId: String): Unit = {
    logInfo("init flow,appId=" + appId)

    // 创建zk目录 /wormhole/${stream_id}/flow，在其目录下创建的节点是对应flow的内容
    val watchPath = WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + flowRelativePath
    if(!WormholeZkClient.checkExist(config.zookeeper_path, watchPath))WormholeZkClient.createPath(config.zookeeper_path, watchPath)
    val flowList = WormholeZkClient.getChildren(config.zookeeper_path, watchPath) // 得到/wormhole/${stream_id}/flow目录下的所有节点。一个节点对应一个flow
    flowList.toArray.foreach(flow => {
      val flowContent = WormholeZkClient.getData(config.zookeeper_path, watchPath + "/" + flow)  // 得到flowContent，为ums结构的字符串
      // 将该flowContent,解析为ums对象，根据该对象的内容启动不同的flow
      add(config.kafka_output.feedback_topic_name,config.kafka_output.brokers)(watchPath + "/" + flow, new String(flowContent))
    })

    WormholeZkClient.setPathChildrenCacheListener(config.zookeeper_path, watchPath, add(config.kafka_output.feedback_topic_name,config.kafka_output.brokers), remove(config.kafka_output.brokers), update(config.kafka_output.feedback_topic_name,config.kafka_output.brokers))
  }

  /**
    *
    * @param feedbackTopicName  反馈topic
    * @param brokers            反馈topic所在的brokers
    * @param path               flow目录下的某个节点
    * @param data               该节点的内容
    * @param time               [没用到]
    */
  def add(feedbackTopicName: String,brokers:String)(path: String, data: String, time: Long = 1): Unit = {
    try {
      logInfo("add"+data)
      // 简单判断是否是ums
      if (!data.startsWith("{")) {
        logWarning("data is " + data + ", not in ums")
      } else {
        // data是ums结构的字符串,返回ums结构体对象
        val ums = UmsSchemaUtils.toUms(data) //flow ums结构对象
        // 根据ums protocol的type不同，启动不同的处理方式
        ums.protocol.`type` match {
          case UmsProtocolType.DIRECTIVE_FLOW_START | UmsProtocolType.DIRECTIVE_FLOW_STOP => // directive_flow_start  directive_flow_stop
            //
            BatchflowDirective.flowStartProcess(ums, feedbackTopicName, brokers)
          case UmsProtocolType.DIRECTIVE_ROUTER_FLOW_START | UmsProtocolType.DIRECTIVE_ROUTER_FLOW_STOP => // directive_router_flow_start  directive_router_flow_stop
            RouterDirective.flowStartProcess(ums, feedbackTopicName, brokers)
          case UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_STOP => // directive_hdfslog_flow_start directive_hdfslog_flow_stop
            HdfsDirective.flowStartProcess(ums, feedbackTopicName, brokers) //TOdo change name uniform, take directiveflowwatch and directiveoffsetwatch out of core, because hdfs also use them
          case _ => logWarning("ums type: " + ums.protocol.`type` + " is not supported")
        }
      }
    } catch {
      case e: Throwable => logAlert("flow add error:" + data, e)
    }
  }

  def remove(brokers:String)(path: String): Unit = {
    try {
      val (streamType,sourceNamespace, sinkNamespace) = getNamespaces(path)
      StreamType.streamType(streamType) match {
        case StreamType.BATCHFLOW => ConfMemoryStorage.cleanDataStorage(sourceNamespace, sinkNamespace)
        case StreamType.HDFSLOG => HdfsMainProcess.directiveNamespaceRule.remove(sourceNamespace)
        case StreamType.ROUTER => RouterMainProcess.removeFromRouterMap(sourceNamespace,sinkNamespace)
        //case _=> logAlert("unsolve stream type:" + StreamType.ROUTER.toString)
      }
    } catch {
      case e: Throwable => logAlert("flow remove error:", e)
    }
  }

  def update(feedbackTopicName: String,brokers:String)(path: String, data: String, time: Long): Unit = {
    try {
      logInfo("update"+data)
      add(feedbackTopicName,brokers)(path, data, time)
    } catch {
      case e: Throwable => logAlert("flow update error:" + data, e)
    }
  }

  private def getNamespaces(path: String): (String, String,String) = {
    val result = path.substring(path.lastIndexOf("/")+1).split("->")
    (result(0).toLowerCase, result(1).toLowerCase,result(2))
  }
}
