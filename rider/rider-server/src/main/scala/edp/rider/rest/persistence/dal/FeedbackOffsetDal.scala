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

import edp.rider.common.RiderLogger
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils.minTimeOut
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.{Await, Future}

class FeedbackOffsetDal(feedbackOffsetTable: TableQuery[FeedbackOffsetTable]) extends BaseDalImpl[FeedbackOffsetTable, FeedbackOffset](feedbackOffsetTable) with RiderLogger {

  def getLatestOffset(streamId: Long, topic: String): Future[Option[FeedbackOffset]] = {
    db.run(feedbackOffsetTable.filter(str => str.streamId === streamId && str.topicName === topic).sortBy(_.feedbackTime.desc).result.headOption)
  }

  def getDistinctStreamTopicList(streamId: Long): Future[Seq[StreamTopicPartitionId]] = {
    db.run(feedbackOffsetTable.filter(str => str.streamId === streamId).
      map { case (str) => (str.streamId, str.topicName, str.partitionNum) <> (StreamTopicPartitionId.tupled, StreamTopicPartitionId.unapply)
      }.distinct.result).mapTo[Seq[StreamTopicPartitionId]]
  }

  def getDistinctList: Future[Seq[IdStreamTopicPartitionId]] = {
    db.run(feedbackOffsetTable.map { case (str) => (str.streamId, str.topicName) <> (IdStreamTopicPartitionId.tupled, IdStreamTopicPartitionId.unapply) }
      .distinct.result).mapTo[Seq[IdStreamTopicPartitionId]]
  }


  def deleteHistory(pastNdays: String, reservedIds: Seq[Long]) = {
    super.deleteByFilter(str => str.feedbackTime <= pastNdays && !str.id.inSet(reservedIds))
  }

  //  def getFeedbackTopicOffset(topicName: String): String = {
  //    val offsetSeq = Await.result(db.run(feedbackOffsetQuery.withFilter(_.topicName === topicName).sortBy(_.feedbackTime.desc).take(1).result).mapTo[Seq[FeedbackOffset]], Duration.Inf)
  //    if (offsetSeq.isEmpty) KafkaUtils.getKafkaLatestOffset(RiderConfig.consumer.brokers, topicName)
  //    else offsetSeq.head.partitionOffsets
  //  }

//  select *
//  from feedback_stream_offset
//  where stream_id = ${streamId}
//  order by feedback_time desc
//  limit ${topicsNum}+1
  // 该streamId的按feedbackTime取最大的n条数据
  def getStreamTopicsFeedbackOffset(streamId: Long, topicsNum: Long) = {
    Await.result(db.run(feedbackOffsetTable.filter(_.streamId === streamId).sortBy(_.feedbackTime.desc).take(topicsNum + 1).result).mapTo[Seq[FeedbackOffset]], minTimeOut)
  }

}