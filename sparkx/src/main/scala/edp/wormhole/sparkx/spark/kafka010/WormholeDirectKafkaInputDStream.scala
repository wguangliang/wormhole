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



package org.apache.spark.streaming.kafka010

import edp.wormhole.sparkx.spark.kafka010.WormholePerPartitionConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class WormholeDirectKafkaInputDStream[K, V](
                                             _ssc: StreamingContext,
                                             locationStrategy: LocationStrategy,
                                             consumerStrategy: ConsumerStrategy[K, V],
                                             ppc: PerPartitionConfig
                                           ) extends DirectKafkaInputDStream[K, V](_ssc, locationStrategy, consumerStrategy, ppc) {

  @transient private var ekc: Consumer[K, V] = null

  def getStreamingContext(): StreamingContext ={
    _ssc
  }

  override def stop(): Unit = this.synchronized {
    if (ekc != null) {
      ekc.close()
    }
  }

  override def consumer(): Consumer[K, V] = this.synchronized {
    if (null == ekc) {
      ekc = consumerStrategy.onStart(currentOffsets.mapValues(l => new java.lang.Long(l)).asJava)
    }
    ekc
  }

  /**
    * 更新topic，并消费到指定offset
    * 增加或者删除topic
    * @param addTopicOffset 需要增加的:Map[(topic, partition id)->(offset, max rate per partition)]
    * @param delTopic 需要删除的topics:set[String]  topic
    */
  def updateTopicOffset(addTopicOffset: Map[(String, Int), (Long, Long)] = Map[(String, Int), (Long, Long)](),
                        delTopic: Set[String] = Set[String]()): Unit = this.synchronized {
    //first Long is offset,second Long is rate
    //                                                                                                            TopicPartition(topic, partition id)->(offset, max rate per partition)
    // 需要增加的Map[TopicPartition(topic, partition id)->(offset, max rate per partition)]
    val addTpOffset: Map[TopicPartition, (Long, Long)] = if (addTopicOffset != null) addTopicOffset.map(tp => new TopicPartition(tp._1._1, tp._1._2) -> (tp._2._1, tp._2._2)) else Map.empty[TopicPartition, (Long, Long)]
    // 先前的Map[TopicPartition(topic, partition id) -> (offset, 分区最大拉取数据量)]
    val prevTpOffset: Map[TopicPartition, (Long, Long)] = currentOffsets.map(elem => {
      elem._1 -> (elem._2, ppc.maxRatePerPartition(elem._1))
    })

    // 构建完成后的Map[TopicPartition(topic, partition id)->(offset, max rate per partition)]结构
    val curTpOffset: Map[TopicPartition, (Long, Long)] = if (delTopic != null && delTopic.nonEmpty) { // 如果有需要删除的topic
      // 先前的结构增加需要增加的topic，并删除要删除的topic的信息
      (prevTpOffset ++ addTpOffset).filter(tp => {
        !delTopic.contains(tp._1.topic())
      })
    } else prevTpOffset ++ addTpOffset

    logWarning("updateTopicOffset before topic " + prevTpOffset)
    logWarning("updateTopicOffset addtopic:" + addTpOffset + " deltopic:" + delTopic)
    logWarning("updateTopicOffset after topic " + curTpOffset)

    //if kc is null,  create reconfig kafka consumer - rkc
    val rkc = consumer // kafka Consumer 对象

    //check input topic valid or not, new topic must create first
    // 当前kafka topic消费者consumer列表
    val rkcTpList = ListBuffer.empty[TopicPartition]
    // listTopics显示出该kafka broker下的所有topic
    rkc.listTopics.asScala.foreach(partitions => {  // listTopics：Map<String, List<PartitionInfo>>
      val topicName = partitions._1
      partitions._2.asScala.foreach(partition => {
        rkcTpList += new TopicPartition(topicName, partition.partition())
      })
    })
    val invalidTopics = curTpOffset.keySet.diff(rkcTpList.toSet)
    if (0 != invalidTopics.size) {
      logWarning("input a invalid topics: " + invalidTopics)
      throw new Exception("input invalid topics:" + invalidTopics)
    }

    val oldPerPartitionRate = ppc.asInstanceOf[WormholePerPartitionConfig].getPerPartitionRate //mutable.HashMap[TopicPartition,Long]；key：TopicPartition(topic,partition id) value:最大拉取数据
    // 设置新的perPartitionRate
    try {
      ppc.asInstanceOf[WormholePerPartitionConfig].setPerPartitionRate(curTpOffset.map(tp => {
        // (TopicPartition(topic,partition id), 最大拉取数据量)
        (tp._1, tp._2._2)
      }))
      // 定阅topic数组
      rkc.subscribe(curTpOffset.keySet.map(_.topic()).asJava)
      // timeout(ms): buffer 中的数据未就绪情况下，等待的最长时间，如果设置为0，立即返回 buffer 中已经就绪的数据
      //从订阅的 partition 中拉取数据,pollOnce() 才是对 Consumer 客户端拉取数据的核心实现
      // consumer poll方法主要做了一下几件事
      //      检查这个 consumer 是否订阅的相应的 topic-partition；
      //      调用 pollOnce() 方法获取相应的 records；
      //      在返回获取的 records 前，发送下一次的 fetch 请求，避免用户在下次请求时线程 block 在 pollOnce() 方法中；
      //      如果在给定的时间（timeout）内获取不到可用的 records，返回空数据。
      rkc.poll(0)
      /**
        * seek相关的方法有3个
        * /**
        * @see KafkaConsumer#seek(TopicPartition, long)
        */
           public void seek(TopicPartition partition, long offset);  指定新的消费位点

         /**
        * @see KafkaConsumer#seekToBeginning(Collection)
        */
          public void seekToBeginning(Collection<TopicPartition> partitions);  定位到最早的offset

        /**
        * @see KafkaConsumer#seekToEnd(Collection)
        */
          public void seekToEnd(Collection<TopicPartition> partitions);   定位到最近的offset Seek to the last offset for each of the given partitions
        */
      //check offset valid or not
      rkc.seekToEnd(curTpOffset.keySet.asJava)
      //get the max offset end
      // consumer position方法： Get the offset of the next record that will be fetched
      // curTpOffsetEnd即partition最大offset的下一个位点
      val curTpOffsetEnd: Map[TopicPartition, Long] = curTpOffset.keySet.map(tp => new TopicPartition(tp.topic(), tp.partition()) -> rkc.position(tp)).toMap
      val invalidAddTpOffset = curTpOffset.filter(elem => {
        if (curTpOffsetEnd.contains(elem._1)) curTpOffsetEnd(elem._1) < elem._2._1 //如果curTpOffsetEnd的比 想要消费的curTpOffset还要小，返回true，计入invalidAddTpOffset。理论上是 最大能消费到的offset：curTpOffsetEnd(elem._1) > elem._2._1
        else true
      })
      if (0 != invalidAddTpOffset.size) { // 如果有不合理的想要达到的offset
        logError("input invalid offset: " + invalidAddTpOffset + " end:" + curTpOffsetEnd)
        throw new Exception("input invalid topics:" + invalidAddTpOffset + " end:" + curTpOffsetEnd)
      }
    }
    catch {
      case any: Exception =>
        logError("check failed for catching:", any)
        logError("stack is: " + any.printStackTrace())
        ppc.asInstanceOf[WormholePerPartitionConfig].setPerPartitionRate(oldPerPartitionRate.toMap)
        rkc.subscribe(prevTpOffset.keySet.map(_.topic()).asJava)
        rkc.poll(0)
        currentOffsets.foreach { case (topicPartition, offset) =>
          rkc.seek(topicPartition, offset)
        }
        throw any
    }

    logWarning("updateTopicOffset before topicoffset " + currentOffsets)

    val backupOffset: Map[TopicPartition, Long] = currentOffsets
    try {
      //update to currentOffsets 更新当前消费所有offset
      currentOffsets = curTpOffset.map(tp => {
        (tp._1, tp._2._1)
      })
      logWarning("updateTopicOffset after topicoffset " + currentOffsets)
      //set to seek position
      currentOffsets.foreach { case (topicPartition, offset) =>
        rkc.seek(topicPartition, offset) // 消费到设置的offset
      }
    }
    catch {
      case any: Exception =>
        // 回退
        currentOffsets = backupOffset //set back to previous value
        ppc.asInstanceOf[WormholePerPartitionConfig].setPerPartitionRate(oldPerPartitionRate.toMap)
        rkc.subscribe(prevTpOffset.keySet.map(_.topic()).asJava)
        rkc.poll(0)
        currentOffsets.foreach { case (topicPartition, offset) =>
          rkc.seek(topicPartition, offset)
        }
        logError("set back offset for catching: ", any)
        throw any
    }
    finally {
      logWarning("updateTopicOffset seek to end")
    }


  }

}
