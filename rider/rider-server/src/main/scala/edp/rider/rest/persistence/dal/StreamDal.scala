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


package edp.rider.rest.persistence.dal

import edp.rider.RiderStarter.modules._
import edp.rider.common.Action._
import edp.rider.common._
import edp.rider.kafka.KafkaUtils._
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.StreamUtils._
import edp.wormhole.util.DateUtils
import edp.wormhole.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class StreamDal(streamTable: TableQuery[StreamTable],
                instanceDal: InstanceDal,
                streamInTopicDal: StreamInTopicDal,
                streamUdfDal: RelStreamUdfDal,
                projectTable: TableQuery[ProjectTable]) extends BaseDalImpl[StreamTable, Stream](streamTable) with RiderLogger {
  /**
    * 根据stream找到关联的project
    * @param streamSeq
    * @return 关联(project id, project name)
    */
  def getStreamProjectMap(streamSeq: Seq[Stream]): Map[Long, String] = {
    // 找到这些stream关联的project
    val projectSeq = Await.result(db.run(projectTable.filter(_.id inSet streamSeq.map(_.projectId)).result).mapTo[Seq[Project]], minTimeOut)
    projectSeq.map(project => (project.id, project.name)).toMap
  }

  def refreshStreamStatus(streamId: Long): Option[Stream] = {
    refreshStreamStatus(streamIdsOpt = Option(Seq(streamId))).headOption
  }

  def refreshStreamStatus(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[Stream] = {
    // 根据project id 和stream ids查询出所有的streams
    val streamSeq = getStreamSeq(projectIdOpt, streamIdsOpt)
    // (streamId -> xxx)
    val streamMap = streamSeq.map(stream => (stream.id, (stream.sparkAppid, stream.status, getStreamTime(stream.startedTime), getStreamTime(stream.stoppedTime)))).toMap
    // 根据log与yarn得到app status状态
    val refreshStreamSeq = getStatus(action, streamSeq) // stream app status的信息
    // 过滤出需要更新的
    val updateStreamSeq = refreshStreamSeq.filter(stream => {
      // 如果完全相等，返回false。即没有更新
      if (streamMap(stream.id) == (stream.sparkAppid, stream.status, getStreamTime(stream.startedTime), getStreamTime(stream.stoppedTime))) false else true
    })
    // 更新数据库stream表
    updateByRefresh(updateStreamSeq)
    refreshStreamSeq // stream app status的信息返回
  }

  /**
    * 根据streamIds查询Stream表得到Seq[Stream]
    * @param projectIdOpt
    * @param streamIdsOpt
    * @return
    */
  def getStreamSeq(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None): Seq[Stream] = {
//    select *
//    from stream
//    where stream_id in $(streamIds)
    (projectIdOpt, streamIdsOpt) match {
      case (_, Some(streamIds)) => Await.result(super.findByFilter(stream => stream.id inSet streamIds), minTimeOut)
      case (Some(projectId), None) => Await.result(super.findByFilter(_.projectId === projectId), minTimeOut)
      case (None, None) => Await.result(super.findAll, minTimeOut)
    }
  }

  /**
    * 根据project id和stream id（或许没有）确定stream
    * 通过log和yarn 确定stream 的状态信息
    * 通过steam关联instance 和 project
    * 封装为StreamDetail
    * @param projectIdOpt
    * @param streamIdsOpt
    * @param action
    * @return
    */
  def getBriefDetail(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[StreamDetail] = {
    try {
      // 刷新stream的状态
      val streamSeq = refreshStreamStatus(projectIdOpt, streamIdsOpt, action) // 得到stream app status的信息
      // 根据stream_id和instance_id与instance表关联得到(streamId, Instance信息)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])  // (stream_id, StreamKafka(instance.nsInstance, instance.connUrl))
      // 找到这些stream关联的project
      val projectMap = getStreamProjectMap(streamSeq)
      // 将关联的project和instance信息封装为StreamDetail
      streamSeq.map(
        stream => {
          //                                                                                                                不可用action                                     隐藏action
          StreamDetail(stream, projectMap(stream.projectId), streamKafkaMap(stream.id), None, Seq[StreamUdf](), getDisableActions(stream.streamType, stream.status), getHideActions(stream.streamType))
        }
      )
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream detail failed", ex)
        throw GetStreamDetailException(ex.getMessage, ex.getCause)
    }
  }

  def getSimpleStreamInfo(projectId: Long, streamType: String, functionTypeOpt: Option[String] = None): Seq[SimpleStreamInfo] = {
    if (streamType.equals("flink")) {
      val streamSeq = Await.result(streamDal.findByFilter(stream => stream.streamType === "flink" && stream.projectId === projectId), minTimeOut)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val startConfigMap = streamSeq.map(stream => (stream.id, stream.startConfig)).toMap[Long, String]
      val configMap = streamSeq.map(stream => (stream.id, json2caseClass[FlinkResourceConfig](startConfigMap(stream.id)))).toMap[Long, FlinkResourceConfig]
      val maxParallelismMap = streamSeq.map(stream => (stream.id, configMap(stream.id).taskManagersNumber * configMap(stream.id).perTaskManagerSlots)).toMap[Long, Int]
      streamSeq.map(
        stream => {
          SimpleStreamInfo(stream.id, stream.name, maxParallelismMap(stream.id), streamKafkaMap(stream.id).instance, getStreamTopicsName(stream.id)._2)
        }
      )
    } else {
      val streamSeq = Await.result(streamDal.findByFilter(stream => stream.streamType === "spark" && stream.projectId === projectId && stream.functionType === functionTypeOpt.get), minTimeOut)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      streamSeq.map(
        stream => {
          SimpleStreamInfo(stream.id, stream.name, 0, streamKafkaMap(stream.id).instance, getStreamTopicsName(stream.id)._2)
        }
      )
    }
  }

  def getStreamDetail(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[StreamDetail] = {
    try {
      // 根据projectid和streamid 查询出来stream，更改保存在cacheMap中该stream的状态为refresh
      val streamSeq = refreshStreamStatus(projectIdOpt, streamIdsOpt, action)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val streamIds = streamSeq.map(_.id)
      val streamTopicMap = getStreamTopicsMap(streamIds)
      val streamUdfSeq = streamUdfDal.getStreamUdf(streamIds)
      //      val streamZkUdfSeq = getZkStreamUdf(streamIds)
      val projectMap = getStreamProjectMap(streamSeq)

      streamSeq.map(
        stream => {
          //          val topics = streamTopicSeq.filter(_.streamId == stream.id).map(
          //            topic => StreamTopic(topic.id, topic.name, topic.partitionOffsets, topic.rate)
          //          )
          val udfs = streamUdfSeq.filter(_.streamId == stream.id).map(
            udf => StreamUdf(udf.id, udf.functionName, udf.fullClassName, udf.jarName)
          )
          //          val zkUdfs = streamZkUdfSeq.filter(_.streamId == stream.id).map(
          //            udf => StreamZkUdf(udf.functionName, udf.fullClassName, udf.jarName)
          //          )
          StreamDetail(stream, projectMap(stream.projectId), streamKafkaMap(stream.id), Option(streamTopicMap(stream.id)), udfs, getDisableActions(stream.streamType, stream.status), getHideActions(stream.streamType))
        }
      )
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream detail failed", ex)
        throw GetStreamDetailException(ex.getMessage, ex.getCause)
    }

  }

  def updateByPutRequest(putStream: PutStream, userId: Long): Future[Int] = {
    db.run(streamTable.filter(_.id === putStream.id)
      .map(stream => (stream.desc, stream.streamConfig, stream.startConfig, stream.launchConfig, stream.updateTime, stream.updateBy))
      .update(putStream.desc, putStream.streamConfig, putStream.startConfig, putStream.launchConfig, currentSec, userId)).mapTo[Int]
  }

  def updateByStatus(streamId: Long, status: String, userId: Long, logPath: String): Future[Int] = {

    if (status == StreamStatus.STARTING.toString) {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.sparkAppid, stream.logPath, stream.startedTime, stream.stoppedTime, stream.updateTime, stream.updateBy))
        //            sparkAppid为null                      stoppedTime为null
        .update(status, null, Some(logPath), Some(currentSec), null, currentSec, userId)).mapTo[Int]
    } else {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.updateTime, stream.updateBy))
        .update(status, currentSec, userId)).mapTo[Int]
    }
  }

  def updateByRefresh(streams: Seq[Stream]): Seq[Int] = {
    streams.map(stream =>
      Await.result(db.run(streamTable.filter(_.id === stream.id)
        .map(stream => (stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime))
        .update(stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime)).mapTo[Int], minTimeOut))
  }

  def getResource(projectId: Long): Future[Resource] = {
    try {
      val project = Await.result(db.run(projectTable.filter(_.id === projectId).result).mapTo[Seq[Project]], minTimeOut).head
      val streamSeq = super.findByFilter(stream => stream.projectId === projectId && (stream.status === "running" || stream.status === "waiting" || stream.status === "starting" || stream.status === "stopping")).mapTo[Seq[Stream]]
      val totalCores = project.resCores
      val totalMemory = project.resMemoryG
      var usedCores = 0
      var usedMemory = 0
      val streamResources = Await.result(streamSeq.map[Seq[AppResource]] {
        streamSeq =>
          streamSeq.sortBy(_.id).map {
            stream =>
              val config = json2caseClass[StartConfig](stream.startConfig)
              usedCores += config.driverCores + config.executorNums * config.perExecutorCores
              usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
              AppResource(stream.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)
          }
      }, minTimeOut)
      Future(Resource(totalCores, totalMemory, totalCores - usedCores, totalMemory - usedMemory, streamResources))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream resource information by project id $projectId failed", ex)
        throw ex
    }
  }

  /**
    * 获取stream表中所有active的stream，封装到StreamCacheMap对象中
    * @return
    */
  def getAllActiveStream: Future[Seq[StreamCacheMap]] = {
    db.run(streamTable.filter(_.active === true).map { case (sTable) =>
      (sTable.id, sTable.name, sTable.projectId) <> (StreamCacheMap.tupled, StreamCacheMap.unapply)
    }.result).mapTo[Seq[StreamCacheMap]]
  }

  def getStreamNameByStreamID(streamId: Long): Future[Stream] = {
    db.run(streamTable.filter(_.active === true).filter(_.id === streamId).result.head).mapTo[Stream]
  }

  def getActiveStreamByProjectId(projectId: Long): Future[Seq[StreamCacheMap]] = {
    db.run(streamTable.filter(_.active === true).filter(_.projectId === projectId).map { case (sTable) =>
      (sTable.id, sTable.name, sTable.projectId) <> (StreamCacheMap.tupled, StreamCacheMap.unapply)
    }.result).mapTo[Seq[StreamCacheMap]]
  }


  def updateStreamTable(stream: Stream): Future[Int] = {
    db.run(streamTable.filter(_.id === stream.id).update(stream))
  }

  def updateStreamsTable(streams: Seq[Stream]) = {
    db.run(DBIO.seq(streams.map(stream => streamTable.filter(_.id === stream.id).update(stream)): _*))
  }

  /**
    * 统计该project下的所有stream所占用的资源：核数和内存
    * @param projectId
    * @return
    */
  def getProjectStreamsUsedResource(projectId: Long) = {
//    select *
//    from stream
//    where project_id = ${projectId} and status in ("running","waiting","starting","stopping")
    val streamSeq: Seq[Stream] = Await.result(super.findByFilter(job => job.projectId === projectId && (job.status === "running" || job.status === "waiting" || job.status === "starting" || job.status === "stopping")), minTimeOut)
    var usedCores = 0
    var usedMemory = 0
    val streamResources: Seq[AppResource] = streamSeq.map(
      stream => {
        StreamType.withName(stream.streamType) match {
          case StreamType.SPARK =>
            val config = json2caseClass[StartConfig](stream.startConfig)
            usedCores += config.driverCores + config.executorNums * config.perExecutorCores
            usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
            AppResource(stream.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)
          case StreamType.FLINK =>
            val config = json2caseClass[FlinkResourceConfig](stream.startConfig)
            usedCores += config.taskManagersNumber * config.perTaskManagerSlots
            usedMemory += config.jobManagerMemoryGB + config.taskManagersNumber * config.perTaskManagerSlots
            AppResource(stream.name, 0, config.jobManagerMemoryGB, config.taskManagersNumber, config.perTaskManagerMemoryGB, config.perTaskManagerSlots)
        }
      }
    )
    (usedCores, usedMemory, streamResources)
  }

  def checkTopicExists(streamId: Long, topic: String): Boolean = {
    var exist = false
    if (streamInTopicDal.checkAutoRegisteredTopicExists(streamId, topic) || streamUdfTopicDal.checkUdfTopicExists(streamId, topic))
      exist = true
    exist
  }


  /**
    * 根据streamId获取kafka instance的信息
    * @param streamId
    * @return
    */
  // get kafka instance id, url
  def getKafkaInfo(streamId: Long): (Long, String) = {
    Await.result(db.run((streamQuery.filter(_.id === streamId) join instanceQuery on (_.instanceId === _.id))
      .map {
        case (_, instance) => (instance.id, instance.connUrl)
      }.result.head).mapTo[(Long, String)], minTimeOut)
  }

  // 根据streamId查找对应的kafka instance的 连接url，组成map
  // key：streamId, value: kafkaUrl
//  select a.id, instance.conn_url
//  from
//  (
//    select id, instance_id,
//    from stream
//      where id in ($streamIds)
//  ) a, instance
//  where a.instance_id = instance.id
  def getStreamKafkaMap(streamIds: Seq[Long]): Map[Long, String] = {
    Await.result(db.run((streamQuery.filter(_.id inSet streamIds) join instanceQuery on (_.instanceId === _.id))
      .map {
        case (stream, instance) => (stream.id, instance.connUrl) <> (StreamIdKafkaUrl.tupled, StreamIdKafkaUrl.unapply)
      }.result).mapTo[Seq[StreamIdKafkaUrl]], minTimeOut)
      .map(streamKafka => (streamKafka.streamId, streamKafka.kafkaUrl)).toMap
  }

  /**
    *
    * @param streamId
    * @return
    */
  def getTopicsAllOffsets(streamId: Long): GetTopicsResponse = {
    getStreamTopicsMap(Seq(streamId))(streamId)
  }

  def getStreamTopicsMap(streamIds: Seq[Long]): Map[Long, GetTopicsResponse] = {
    // 查找与streamIds的stream的一些topic信息，且这些topic必须在ns_database存在
    // Seq[StreamTopicTemp]
    val autoRegisteredTopics = streamInTopicDal.getAutoRegisteredTopics(streamIds)

    // 用户自定义的topic
    // Seq[StreamTopicTemp]
    val udfTopics = streamUdfTopicDal.getUdfTopics(streamIds)

    // 根据streamId查找对应的kafka instance的 连接url，组成map
    // key：streamId, value: kafkaUrl
    val kafkaMap = getStreamKafkaMap(streamIds)
    streamIds.map(id => {
      // 上面两个Seq[StreamTopicTemp]合并
      val topics = autoRegisteredTopics.filter(_.streamId == id) ++: udfTopics.filter(_.streamId == id)
      // topicName -> 最大的消费partitonOffset
      val feedbackOffsetMap = getConsumedMaxOffset(id, topics)

      // 结果
      //                                     合并topic            kakfa连接信息  topic最大消费offset
      val autoTopicsResponse = genAllOffsets(autoRegisteredTopics, kafkaMap, feedbackOffsetMap)
      val udfTopicsResponse = genAllOffsets(udfTopics, kafkaMap, feedbackOffsetMap)

      //update offset in table
      val autoRegisteredUpdateTopics = autoTopicsResponse.map(topic => UpdateTopicOffset(topic.id, topic.consumedLatestOffset)) // 封装为UpdateTopicOffset
      val udfUpdateTopics = udfTopicsResponse.map(topic => UpdateTopicOffset(topic.id, topic.consumedLatestOffset))             // 封装为UpdateTopicOffset

      streamInTopicDal.updateOffset(autoRegisteredUpdateTopics)  // 更新 rel_stream_intopic表id对应的partition_offsets
      streamUdfTopicDal.updateOffset(udfUpdateTopics)            // 更新 rel_stream_userdefined_topic表id对应的partition_offsets
      (id, GetTopicsResponse(autoTopicsResponse, udfTopicsResponse))
    }).toMap
    //    GetTopicsResponse(autoRegisteredTopicsResponse, udfTopicsResponse)
  }
    //getStreamTopicsName for getSimpleStreamInfo
  def getStreamTopicsName(streamIds: Long): (Long, Seq[String]) = {
    val autoRegisteredTopics = streamInTopicDal.getAutoRegisteredTopics(streamIds)
    val udfTopics = streamUdfTopicDal.getUdfTopics(streamIds)
    val topics = autoRegisteredTopics.filter(_.streamId == streamIds) ++: udfTopics.filter(_.streamId == streamIds)
    val topicsName = topics.map(topics => topics.name)
    val topicInfo = (streamIds, topicsName)
    topicInfo
  }

  //                               合并topic            kakfa连接信息               topic最大消费offset
  def genAllOffsets(topics: Seq[StreamTopicTemp], kafkaMap: Map[Long, String], feedbackOffsetMap: Map[String, String]): Seq[TopicAllOffsets] = {
    // 遍历合并的topic
    topics.map(topic => {
      val earliest = getKafkaEarliestOffset(kafkaMap(topic.streamId), topic.name)   // 最小
      val latest = getKafkaLatestOffset(kafkaMap(topic.streamId), topic.name)       // 最大
      val consumed = formatConsumedOffsetByLatestOffset(feedbackOffsetMap(topic.name), latest) // 以feedbackOffset为主，进行截取或者扩充
      TopicAllOffsets(topic.id, topic.name, topic.rate, consumed, earliest, latest)  // 封装为TopicAllOffsets对象进行返回
    })
  }

  def getConsumedMaxOffset(streamId: Long, topics: Seq[StreamTopicTemp]): Map[String, String] = {
    try {
      // 查找streamId对应的stream信息，取第一个
      val stream = Await.result(super.findById(streamId), minTimeOut).head

      // 该streamId的按feedbackTime取最大的n条数据
      val topicFeedbackSeq = feedbackOffsetDal.getStreamTopicsFeedbackOffset(streamId, topics.size)

      // 初始化topicOffsetMap
      val topicOffsetMap = new mutable.HashMap[String, String]()
      // 取feedback最大的合法数据的topic
      topicFeedbackSeq.foreach(topic => {
        // 如果不包含topicName
        if (!topicOffsetMap.contains(topic.topicName)) {
          if (stream.startedTime.nonEmpty && stream.startedTime != null &&
            DateUtils.yyyyMMddHHmmss(topic.umsTs) > DateUtils.yyyyMMddHHmmss(stream.startedTime.get)) // ums_ts > startTime
            // 添加进去
            topicOffsetMap(topic.topicName) = formatOffset(topic.partitionOffsets)
        }
      })
      topics.foreach(
        topic => {
          // 如果topicOffsetMap不包含该topicName，添加该topic:StreamTopicTemp信息添加至topicOffsetMap
          if (!topicOffsetMap.contains(topic.name)) topicOffsetMap(topic.name) = formatOffset(topic.partitionOffsets)
        }
      )
      topicOffsetMap.toMap  // 返回
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream consumed latest offset from feedback failed", ex)
        throw ex
    }
  }


}