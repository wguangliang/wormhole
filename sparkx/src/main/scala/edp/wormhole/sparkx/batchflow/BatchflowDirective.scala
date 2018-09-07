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

import com.alibaba.fastjson.JSON
import edp.wormhole.common.InputDataProtocolBaseType
import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.common.json.{JsonSourceConf, RegularJsonSchema}
import edp.wormhole.sparkx.directive._
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.swifts.parse.ParseSwiftsSql
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, ValidityConfig}
import edp.wormhole.swifts.ConnectionMemoryStorage
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.ums._
import edp.wormhole.util.config.KVConfig
import edp.wormhole.util.{DateUtils, JsonUtils}

import scala.collection.mutable

object BatchflowDirective extends Directive {
  /**
    * 注册
    * @param sourceNamespace          schema.namespace          kafka.kafka-test.kafka-source.ums_extension.*.*.*
    * @param fullsinkNamespace        payload[?].tuple[5]       mysql.mysql-test.mysql-sink.count_num.*.*.*
    * @param streamId                 payload[?].tuple[1]       4
    * @param directiveId              payload[?].tuple[0]       248
    * @param swiftsStr                payload[?].tuple[7]       {"pushdown_connection":[],"dataframe_show":"true","action":"c3Bhcmtfc3FsID0gc2VsZWN0IGNvdW50KGlkKSBhcyBjb3VudCBmcm9tIHVtc19leHRlbnNpb24g\nZ3JvdXAgYnkgbmFtZTs=","dataframe_show_num":10}
    * @param sinksStr                 payload[?].tuple[8]       {"sink_connection_url":"jdbc:mysql://localhost:3306/mysql-sink","sink_connection_username":"wormhole","sink_connection_password":"wormhole","sink_table_keys":"count","sink_output":"","sink_connection_config":"","sink_process_class_fullname":"com.netease.datastream.sinks.hbase.Data2HBaseSink","sink_specific_config":{"mutation_type":"i"},"sink_retry_times":"3","sink_retry_seconds":"300"}
    * @param feedbackTopicName                                  wormhole_feedback
    * @param brokers                                            localhost:9092
    * @param consumptionDataStr       payload[?].tuple[6]       {"initial": true, "increment": true, "batch": false}
    * @param dataType                 payload[?].tuple[3]       ums_extension
    * @param dataParseStr             payload[?].tuple[4]       {"fields":[{"name":"id","type":"long","nullable":true},{"name":"name","type":"string","nullable":true},{"name":"phone","type":"string","nullable":true},{"name":"address","type":"string","nullable":true},{"name":"time","type":"datetime","nullable":true},{"name":"time","type":"datetime","nullable":true,"rename":"ums_ts_"}]}
    */
  private def registerFlowStartDirective(sourceNamespace: String, fullsinkNamespace: String, streamId: Long, directiveId: Long,
                                         swiftsStr: String, sinksStr: String, feedbackTopicName: String, brokers: String,
                                         consumptionDataStr: String, dataType: String, dataParseStr: String): Unit = {
    val consumptionDataMap = mutable.HashMap.empty[String, Boolean]
    val consumption = JSON.parseObject(consumptionDataStr)
    val initial = consumption.getString(InputDataProtocolBaseType.INITIAL.toString).trim.toLowerCase.toBoolean      // initial
    val increment = consumption.getString(InputDataProtocolBaseType.INCREMENT.toString).trim.toLowerCase.toBoolean  // increment
    val batch = consumption.getString(InputDataProtocolBaseType.BATCH.toString).trim.toLowerCase.toBoolean          // batch
    consumptionDataMap(InputDataProtocolBaseType.INITIAL.toString) = initial
    consumptionDataMap(InputDataProtocolBaseType.INCREMENT.toString) = increment
    consumptionDataMap(InputDataProtocolBaseType.BATCH.toString) = batch
    // 解析swifts
    val swiftsProcessConfig: Option[SwiftsProcessConfig] = if (swiftsStr != null) {
      // 解析swifts
      val swifts = JSON.parseObject(swiftsStr)
      if (swifts.size() > 0) {
        val validity = if (swifts.containsKey("validity") && swifts.getString("validity").trim.nonEmpty && swifts.getJSONObject("validity").size > 0) swifts.getJSONObject("validity") else null
        var validityConfig: Option[ValidityConfig] = None
        if (validity != null) {
          val check_columns = validity.getString("check_columns").trim.toLowerCase
          val check_rule = validity.getString("check_rule").trim.toLowerCase
          val rule_mode = validity.getString("rule_mode").trim.toLowerCase
          val rule_params = validity.getString("rule_params").trim.toLowerCase
          val against_action = validity.getString("against_action").trim.toLowerCase
          var i = 0
          if (check_rule.nonEmpty) i += 1
          if (check_columns.nonEmpty) i += 1
          if (rule_mode.nonEmpty) i += 1
          if (rule_params.nonEmpty) i += 1
          if (against_action.nonEmpty) i += 1
          if (!(i == 5 || i == 0)) {
            throw new Exception("rule related fields must be all null or not null ")
          }
          if (i > 0) validityConfig = Some(ValidityConfig(check_columns.split(",").map(_.trim), check_rule, rule_mode, rule_params, against_action))
        }
        // action为sql
        val action: String = if (swifts.containsKey("action") && swifts.getString("action").trim.nonEmpty) swifts.getString("action").trim else null
        val dataframe_show = if (swifts.containsKey("dataframe_show") && swifts.getString("dataframe_show").trim.nonEmpty)
          Some(swifts.getString("dataframe_show").trim.toLowerCase.toBoolean)
        else Some(false)
        val dataframe_show_num: Option[Int] = if (swifts.containsKey("dataframe_show_num"))
          Some(swifts.getInteger("dataframe_show_num")) else Some(20)  // 默认20
        val swiftsSpecialConfig = if (swifts.containsKey("swifts_specific_config")) swifts.getString("swifts_specific_config")
        else ""
        // 连接信息，转换过程中的用到的数据库连接信息
        val pushdown_connection = if (swifts.containsKey("pushdown_connection") && swifts.getString("pushdown_connection").trim.nonEmpty && swifts.getJSONArray("pushdown_connection").size > 0) swifts.getJSONArray("pushdown_connection") else null
        if (pushdown_connection != null) {
          val connectionListSize = pushdown_connection.size()
          for (i <- 0 until connectionListSize) {
            val jsonObj = pushdown_connection.getJSONObject(i)
            val name_space = jsonObj.getString("name_space").trim.toLowerCase
            val jdbc_url = jsonObj.getString("jdbc_url")
            val username = if (jsonObj.containsKey("username")) Some(jsonObj.getString("username")) else None
            val password = if (jsonObj.containsKey("password")) Some(jsonObj.getString("password")) else None
            val parameters = if (jsonObj.containsKey("connection_config") && jsonObj.getString("connection_config").trim.nonEmpty) {
              logInfo("connection_config:" + jsonObj.getString("connection_config"))
              Some(JsonUtils.json2caseClass[Seq[KVConfig]](jsonObj.getString("connection_config")))
            } else {
              logInfo("not contains connection_config")
              None
            }
            // 内存中维护 namespace->connectionconfig对应关系
            ConnectionMemoryStorage.registerDataStoreConnectionsMap(name_space, jdbc_url, username, password, parameters)
          }
        }

        // 解析sql
        val SwiftsSqlArr = if (action != null) {
          val sqlStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(action))
          ParseSwiftsSql.parse(sqlStr, sourceNamespace, fullsinkNamespace, if (validity == null) false else true, dataType)
        } else None
        Some(SwiftsProcessConfig(SwiftsSqlArr, validityConfig, dataframe_show, dataframe_show_num, Some(swiftsSpecialConfig)))
      } else {
        None
      }
    } else None

    val sinks = JSON.parseObject(sinksStr)
    val sink_connection_url = sinks.getString("sink_connection_url").trim.toLowerCase
    val sink_connection_username = if (sinks.containsKey("sink_connection_username")) Some(sinks.getString("sink_connection_username").trim) else None
    val sink_connection_password = if (sinks.containsKey("sink_connection_password")) Some(sinks.getString("sink_connection_password").trim) else None
    val parameters = if (sinks.containsKey("sink_connection_config") && sinks.getString("sink_connection_config").trim.nonEmpty) Some(JsonUtils.json2caseClass[Seq[KVConfig]](sinks.getString("sink_connection_config"))) else None
    val sink_table_keys = if (sinks.containsKey("sink_table_keys") && sinks.getString("sink_table_keys").trim.nonEmpty) Some(sinks.getString("sink_table_keys").trim.toLowerCase) else None
    val sink_specific_config = if (sinks.containsKey("sink_specific_config") && sinks.getString("sink_specific_config").trim.nonEmpty) Some(sinks.getString("sink_specific_config")) else None
    val sink_process_class_fullname = sinks.getString("sink_process_class_fullname").trim
    val sink_retry_times = sinks.getString("sink_retry_times").trim.toLowerCase.toInt
    val sink_retry_seconds = sinks.getString("sink_retry_seconds").trim.toLowerCase.toInt
    val sink_output = if (sinks.containsKey("sink_output") && sinks.getString("sink_output").trim.nonEmpty) {
      var tmpOutput = sinks.getString("sink_output").trim.toLowerCase.split(",").map(_.trim).mkString(",")
      if (dataType == "ums" && tmpOutput.nonEmpty) {
        if (tmpOutput.indexOf(UmsSysField.TS.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.TS.toString
        }
        if (tmpOutput.indexOf(UmsSysField.ID.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.ID.toString
        }
        if (tmpOutput.indexOf(UmsSysField.OP.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.OP.toString
        }
      }
      tmpOutput
    } else ""

    val sink_schema = if (sinks.containsKey("sink_schema") && sinks.getString("sink_schema").trim.nonEmpty) {
      val sinkSchemaEncoded = sinks.getString("sink_schema").trim
      Some(new String(new sun.misc.BASE64Decoder().decodeBuffer(sinkSchemaEncoded.toString)))
      //ConfMemoryStorage.registerJsonSourceSinkSchema(sourceNamespace, fullsinkNamespace, sink_schema)
    } else None

    if (dataType != "ums") { // ums_extension
      // 解析
      val parseResult: RegularJsonSchema = JsonSourceConf.parse(dataParseStr)
      if (initial)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INITIAL_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr) // data_initial_data
      if (increment)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INCREMENT_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr) // data_increment_data
      if (batch)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_BATCH_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr) // data_batch_data

    }
    // 将sinkconfig信息封装
    val sinkProcessConfig = SinkProcessConfig(sink_output, sink_table_keys, sink_specific_config, sink_schema, sink_process_class_fullname, sink_retry_times, sink_retry_seconds)


    val swiftsStrCache = if (swiftsStr == null) "" else swiftsStr


    ConfMemoryStorage.registerStreamLookupNamespaceMap(sourceNamespace, fullsinkNamespace, swiftsProcessConfig)
    ConfMemoryStorage.registerFlowConfigMap(sourceNamespace, fullsinkNamespace, swiftsProcessConfig, sinkProcessConfig, directiveId, swiftsStrCache, sinksStr, consumptionDataMap.toMap)


    ConnectionMemoryStorage.registerDataStoreConnectionsMap(fullsinkNamespace, sink_connection_url, sink_connection_username, sink_connection_password, parameters)
    WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, ""), None, brokers)

  }

  override def flowStartProcess(ums: Ums, feedbackTopicName: String, brokers: String): Unit = {
    val payloads = ums.payload_get                              // payload
    val schemas = ums.schema.fields_get                         // schema
    val sourceNamespace = ums.schema.namespace.toLowerCase      // namespace
    // 遍历payload
    payloads.foreach(tuple => {  // UmsTuple
      // 从payload中找到stream_id,并根据schema进行格式转换
      val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
      // 从payload中找到directive_id，并根据schema进行格式转换
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      try {
        val swiftsEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "swifts")
        // 拿到base64解码后的swift
        val swiftsStr = if (swiftsEncoded != null && !swiftsEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(swiftsEncoded.toString)) else null
        logInfo("swiftsStr:" + swiftsStr)
        // 拿到base64解码后的sink
        val sinksStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sinks").toString))
        logInfo("sinksStr:" + sinksStr)
        // 拿到base64解码后的consumption_protocol
        val consumptionDataStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(tuple.tuple, schemas, "consumption_protocol").toString))
        // 拿到data_type
        val dataType = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
        // 拿到sink namespace
        val fullSinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sink_namespace").toString.toLowerCase
        // 拿到base64解码后的data_parse
        val dataParseEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_parse")
        val dataParseStr = if (dataParseEncoded != null && !dataParseEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(dataParseEncoded.toString)) else null
        // 注册flow启动信息
        registerFlowStartDirective(sourceNamespace, fullSinkNamespace, streamId, directiveId, swiftsStr, sinksStr, feedbackTopicName, brokers, consumptionDataStr, dataType, dataParseStr)
      } catch {
        case e: Throwable =>
          logAlert("registerFlowStartDirective,sourceNamespace:" + sourceNamespace, e)
          // 报错会向反馈feedbacktopic中发送错误信息
          //                                    topic                      partition=0                message                                                                                                 key   brokers
          WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId, e.getMessage), None, brokers)

      }

    })
  }
}
