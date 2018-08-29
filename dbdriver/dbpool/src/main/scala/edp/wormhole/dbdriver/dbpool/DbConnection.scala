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


package edp.wormhole.dbdriver.dbpool

import java.sql.Connection

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import edp.wormhole.util.config.ConnectionConfig

import scala.collection.mutable

/**
  * 使用HikariCP 高性能数据库连接池对Connection进行管理
  */
object DbConnection extends Serializable {
  /**
    * 主要维护dataSourceMap
    * 根据jdbc url和username找到对应的HikariDataSource
    */
  //                                      (jdbc_url, username) => HikariDataSource
  lazy val datasourceMap: mutable.HashMap[(String,String), HikariDataSource] = new mutable.HashMap[(String,String), HikariDataSource]
  /**
    * 根据ConnectionConfig获得Connection，并在dataSourceMap中维护(jdbc_url, username) => HikariDataSource对应关系
    * @param jdbcConfig
    * @return
    */
  def getConnection(jdbcConfig: ConnectionConfig): Connection = {
    val tmpJdbcUrl = jdbcConfig.connectionUrl.toLowerCase
    val tmpUsername = jdbcConfig.username.getOrElse("").toLowerCase
    if (!datasourceMap.contains((tmpJdbcUrl,tmpUsername)) || datasourceMap((tmpJdbcUrl,tmpUsername)) == null) {
      synchronized {
        if (!datasourceMap.contains((tmpJdbcUrl,tmpUsername)) || datasourceMap((tmpJdbcUrl,tmpUsername)) == null) {
          initJdbc(jdbcConfig)
        }
      }
    }
    // 取得一个Connection
    datasourceMap((tmpJdbcUrl,tmpUsername)).getConnection
  }
  /**
    * 私有的，在getConnection中得到调用
    * 初始化连接池，并在dataSourceMap中维护(jdbc_url, username) => HikariDataSource对应关系
    * @param jdbcConfig
    */
  private def initJdbc(jdbcConfig: ConnectionConfig): Unit = {
    val jdbcUrl = jdbcConfig.connectionUrl
    val username = jdbcConfig.username
    val password = jdbcConfig.password
    val kvConfig = jdbcConfig.parameters
    println(jdbcUrl)
    /**
      * 1)创建HikariConfig
      */
    val config = new HikariConfig()
    val tmpJdbcUrl = jdbcUrl.toLowerCase
    // 根据不同的jdbc url，判断不同的driver class name
    // mysql、oracle、postgresql、sqlserver、h2、hbase phoenix、cassandra、mongodb、elasticSearch、vertical
    if (tmpJdbcUrl.indexOf("mysql") > -1) {
      println("mysql")
      config.setConnectionTestQuery("SELECT 1")
      config.setDriverClassName("com.mysql.jdbc.Driver")
    } else if (tmpJdbcUrl.indexOf("oracle") > -1) {
      println("oracle")
      config.setConnectionTestQuery("SELECT 1 from dual")
      config.setDriverClassName("oracle.jdbc.driver.OracleDriver")
    } else if (tmpJdbcUrl.indexOf("postgresql") > -1) {
      println("postgresql")
      config.setDriverClassName("org.postgresql.Driver")
    } else if (tmpJdbcUrl.indexOf("sqlserver") > -1) {
      println("sqlserver")
      config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    } else if (tmpJdbcUrl.indexOf("h2") > -1) {
      println("h2")
      config.setDriverClassName("org.h2.Driver")
    } else if (tmpJdbcUrl.indexOf("phoenix") > -1) {
      println("hbase phoenix")
      config.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver")
    } else if (tmpJdbcUrl.indexOf("cassandra") > -1) {
      println("cassandra")
      config.setDriverClassName("com.github.adejanovski.cassandra.jdbc.CassandraDriver")
    } else if (tmpJdbcUrl.indexOf("mongodb") > -1) {
      println("mongodb")
      config.setDriverClassName("mongodb.jdbc.MongoDriver")
    } else if (tmpJdbcUrl.indexOf("sql4es") > -1) {
      println("elasticSearch")
      config.setDriverClassName("nl.anchormen.sql4es.jdbc.ESDriver")
    } else if (tmpJdbcUrl.indexOf("vertica") > -1) {
      println("vertical")
      config.setDriverClassName("com.vertica.jdbc.Driver")
    }


    config.setUsername(username.getOrElse(""))
    config.setPassword(password.getOrElse(""))
    config.setJdbcUrl(jdbcUrl)
    //    config.setMaximumPoolSize(maxPoolSize)
    config.setMinimumIdle(1)
    // 如果jdbc url不是sql4es
    if(tmpJdbcUrl.indexOf("sql4es") < 0){
      // 设置kv属性
      config.addDataSourceProperty("cachePrepStmts", "true")
      config.addDataSourceProperty("maximumPoolSize", "1")
      config.addDataSourceProperty("prepStmtCacheSize", "250")
      config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    }

    // 根据ConnectionConfig设置额外的kv属性
    if(kvConfig.nonEmpty)  kvConfig.get.foreach(kv => config.addDataSourceProperty(kv.key, kv.value))

    val ds: HikariDataSource = new HikariDataSource(config)
    println(tmpJdbcUrl + "$$$$$$$$$$$$$$$$$" + ds.getUsername + " " + ds.getPassword)
    // 加入 (url, username) => HikariDataSource
    datasourceMap((tmpJdbcUrl,username.getOrElse("").toLowerCase)) = ds
  }

  /**
    * 关闭重新获取HikariDataSource，维护在dataSourceMap中
    * @param jdbcConfig
    */
  def resetConnection(jdbcConfig: ConnectionConfig):Unit = {
    shutdownConnection(jdbcConfig.connectionUrl.toLowerCase,jdbcConfig.username.orNull)
    //    datasourceMap -= jdbcUrl
    getConnection(jdbcConfig).close()
  }
  /**
    * 根据jdbcurl和username找到对应的dataSource，关闭，并在datasourceMap减去
    * @param jdbcUrl
    * @param username
    */
  def shutdownConnection(jdbcUrl: String,username:String):Unit = {
    val tmpJdbcUrl = jdbcUrl.toLowerCase
    val tmpUsername = if(username==null) "" else username.toLowerCase
    datasourceMap((tmpJdbcUrl,tmpUsername)).close()
    datasourceMap -= ((tmpJdbcUrl,tmpUsername))
  }

}
