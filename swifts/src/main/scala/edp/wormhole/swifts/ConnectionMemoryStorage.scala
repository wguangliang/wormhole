package edp.wormhole.swifts

import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import org.apache.log4j.Logger

import scala.collection.mutable

/**
  * 内存维护 namespace->Connection
  */
object ConnectionMemoryStorage {

  private val logger: Logger = Logger.getLogger(ConnectionMemoryStorage.getClass)
  private val dataStoreConnectionsMap = mutable.HashMap.empty[String, ConnectionConfig]


  def getDataStoreConnectionsMap: Map[String, ConnectionConfig] = dataStoreConnectionsMap.toMap

  // 将namespace与对应的ConnectionConfig 的关系存入 dataStoreConnectionsMap
  def registerDataStoreConnectionsMap(lookupNamespace: String, connectionUrl: String, username: Option[String], password: Option[String], parameters: Option[Seq[KVConfig]]) {
    logger.info("register datastore,lookupNamespace:" + lookupNamespace + ",connectionUrl;" + connectionUrl + ",username:" + username + ",password:" + password + ",parameters:" + parameters)
    val connectionNamespace = lookupNamespace.split("\\.").slice(0, 3).mkString(".")
    println("connectionNamespace:" + connectionNamespace)
    if (!dataStoreConnectionsMap.contains(connectionNamespace)) {
      dataStoreConnectionsMap(connectionNamespace) = ConnectionConfig(connectionUrl, username, password, parameters)
      logger.info("register datastore success,lookupNamespace:" + lookupNamespace + ",connectionUrl;" + connectionUrl + ",username:" + username + ",password:" + password + ",parameters:" + parameters)
    }
  }

  /**
    * 在传入的dataStoreConnectionsMap 中查询namespace对应的ConnectionConfig
    * @param dataStoreConnectionsMap
    * @param namespace
    * @return
    */
  def getDataStoreConnectionsWithMap(dataStoreConnectionsMap: Map[String, ConnectionConfig], namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")
    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

  /**
    *  在内存中的dataStoreConnectionsMap 中查询namespace对应的ConnectionConfig
    * @param namespace
    * @return
    */
  def getDataStoreConnectionConfig(namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")

    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

}
