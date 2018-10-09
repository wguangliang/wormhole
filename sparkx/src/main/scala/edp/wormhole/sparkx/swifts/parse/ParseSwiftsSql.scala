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


package edp.wormhole.sparkx.swifts.parse

import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.swifts.{ParseSwiftsSqlInternal, SqlOptType}
import edp.wormhole.util.swifts.SwiftsSql

object ParseSwiftsSql extends EdpLogging {

  /**
    *
    * @param sqlStr
    * @param sourceNamespace
    * @param sinkNamespace
    * @param validity
    * @param dataType
    * @return
    */
  def parse(sqlStr: String,           // sql
            sourceNamespace: String, //sourceNamespace is rule 比如：kafka.kafka-test.kafka-source.ums_extension.*.*.*
            sinkNamespace: String,  // 比如：mysql.mysql-test.mysql-sink.count_num.*.*.*
            validity: Boolean,
            dataType: String): Option[Array[SwiftsSql]] = {
    if (sqlStr.trim.nonEmpty) {
      // 将换行、制表符去除。根据分号拆开多个sql
      val sqlStrArray = sqlStr.trim.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ").split(";").map(str => {
        val trimStr = str.trim
        //  去除开头 pushdown_sql
        if (trimStr.startsWith(SqlOptType.PUSHDOWN_SQL.toString)) trimStr.substring(12).trim
          // 去除开头 parquet_sql
        else if (trimStr.startsWith(SqlOptType.PARQUET_SQL.toString)) trimStr.substring(11).trim
        else trimStr
      })
      val swiftsSqlArr = getSwiftsSql(sqlStrArray, sourceNamespace, sinkNamespace, validity, dataType) //sourcenamespace is rule
      swiftsSqlArr
    } else {
      None
    }
  }

  private def getSwiftsSql(sqlStrArray: Array[String],  // sql array
                           sourceNamespace: String, //sourcenamespace is rule 比如：kafka.kafka-test.kafka-source.ums_extension.*.*.*
                           sinkNamespace: String, // 比如：mysql.mysql-test.mysql-sink.count_num.*.*.*
                           validity: Boolean,
                           dataType: String): Option[Array[SwiftsSql]] = {  // 比如：ums_extension
    val swiftsSqlList = Some(sqlStrArray.map(sqlStrEle => {
      val sqlStrEleTrim = sqlStrEle.trim + " " //to fix no where clause bug, e.g select a, b from table;
      logInfo("sqlStrEle:::" + sqlStrEleTrim)
      if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.JOIN.toString) || sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.INNER_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.LEFT_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.LEFT_JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.RIGHT_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.RIGHT_JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.UNION.toString)) {
        ParseSwiftsSqlInternal.getUnion(sqlStrEleTrim, sourceNamespace, sinkNamespace, validity, dataType)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.SPARK_SQL.toString)) {   // spark_sql= 走这里
        ParseSwiftsSqlInternal.getSparkSql(sqlStrEleTrim, sourceNamespace, validity, dataType)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.CUSTOM_CLASS.toString)) {  //
        getCustomClass(sqlStrEleTrim)
      } else {
        logError("optType:" + sqlStrEleTrim + " is not supported")
        throw new Exception("wong operation data type:" + sqlStrEleTrim)
      }
    })
    )
    swiftsSqlList
  }


  private def getCustomClass(sqlStrEle: String): SwiftsSql = {
    val classFullName = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1).trim
    // className -> (reflectObject, transformMethod) 注册到 swiftsTransformReflectMap
    ConfMemoryStorage.registerSwiftsTransformReflectMap(classFullName)
    SwiftsSql(SqlOptType.CUSTOM_CLASS.toString, None, classFullName, None, None, None, None, None)
  }
}
