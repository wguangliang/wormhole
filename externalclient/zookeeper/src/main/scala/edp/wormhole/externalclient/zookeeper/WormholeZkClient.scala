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


package edp.wormhole.externalclient.zookeeper

import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.data.Stat
/**
  * zookeeper 操作类
  * Curator是Netflix公司一个开源的zookeeper客户端，
  * 在原生API接口上进行了包装，解决了很多ZooKeeper客户端非常底层的细节开发。
  * 同时内部实现了诸如Session超时重连，Watcher反复注册等功能，实现了Fluent风格的API接口，是使用最广泛的zookeeper客户端之一。
  *
  * 监控目录为 val watchPath = WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + flowRelativePath
  *           val udfPath = WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + udfRelativePath
  * 其中  WormholeConstants.CheckpointRootPath = "/wormhole/"
  *       flowRelativePath = "/flow"
  *       udfRelativePath = "/udf"
  *
  * 在zk下主目录为/wormhole，在该主目录下，以stream_id为目录继续划分/wormhole/${stream_id}/[offset, application_1530860253617_0048, flow, udf]
  *
  *
  *
  */
object WormholeZkClient {

  @volatile var zkClient: CuratorFramework = null

  //  lazy val zookeeperPath:String = null
  // 单例设计模式得到CuratorFramework
  def getZkClient(zkAddress: String): CuratorFramework = {
    if (zkClient == null) {
      synchronized {
        if (zkClient == null) {
          val retryPolicy = new ExponentialBackoffRetry(1000, 3)  // 初试时间为1s 重试3次
          zkClient = CuratorFrameworkFactory.newClient(getZkAddress(zkAddress), retryPolicy)
          zkClient.start() // 开启连接
        }
      }
    }
    zkClient
  }
  /**
    * 关闭连接
    */
  def closeZkClient(): Unit ={
    try{
      if (zkClient != null) {
        zkClient.close()
      }
    }catch{
      case e:Throwable=>println("zkClient.close error")
    }
  }
  /**
    * 全局计数器
    * @param zkAddress
    * @param path
    * @return
    */
  def getNextAtomicIncrement(zkAddress: String, path: String): Long = {
    // todo if it failed? try catch not work
    val atomicLong = new DistributedAtomicLong(getZkClient(zkAddress), path, new ExponentialBackoffRetry(1000, 3))
    val newValue = atomicLong.increment()
    if (!newValue.succeeded()) {
      println("getAtomic Increment failed")
    }
    newValue.postValue()
  }

  /**
    * 监控子节点状态
    * @param zkAddress
    * @param path
    * @param add
    * @param remove
    * @param update
    */
  def setPathChildrenCacheListener(zkAddress: String, path: String, add: (String, String, Long) => Unit, remove: String => Unit, update: (String, String, Long) => Unit): Unit = {
    val pathChildrenCache = new PathChildrenCache(getZkClient(zkAddress), getPath(zkAddress, path), true)
    pathChildrenCache.getListenable.addListener(new PathChildrenCacheListener() {
      @Override
      def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
        val data = event.getData
        if (data != null) {
          event.getType match {
            case PathChildrenCacheEvent.Type.CHILD_ADDED =>
              add(data.getPath, new String(data.getData), data.getStat.getMtime)
              println("NODE_ADDED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
              remove(data.getPath)
              println("NODE_REMOVED : " + data.getPath)
            case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
              update(data.getPath, new String(data.getData), data.getStat.getMtime)
              println("CHILD_UPDATED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case _ => println("event.getType=" + event.getType + " is not support")
          }
        } else {
          println("data is null : " + event.getType)
        }
      }
    })
    pathChildrenCache.start()

  }

  /**
    * 监控节点状态
    * @param zkAddress
    * @param path
    * @param add
    * @param remove
    * @param update
    */
  def setNodeCacheListener(zkAddress: String, path: String, add: (String, String, Long) => Unit, remove: String => Unit, update: (String, String, Long) => Unit): Unit = {
    val nodeCache = new NodeCache(getZkClient(zkAddress), getPath(zkAddress, path))
    nodeCache.getListenable.addListener(new NodeCacheListener() {
      @Override
      def nodeChanged() {
        if (nodeCache.getCurrentData != null) {
          val data = nodeCache.getCurrentData
          update(data.getPath, new String(data.getData), data.getStat.getMtime)
          println("NODE_CHANGED: " + data.getPath + ", content: " + new String(data.getData))
        }
      }
    })
    nodeCache.start()

  }

  /**
    * 监控节点及子节点的状态
    * @param zkAddress
    * @param path
    * @param add
    * @param remove
    * @param update
    */
  //NOT USE YET
  def setTreeCacheListener(zkAddress: String, path: String, add: (String, String, Long) => Unit, remove: String => Unit, update: (String, Array[String], Long) => Unit): Unit = {
    val treeCache = new TreeCache(getZkClient(zkAddress), getPath(zkAddress, path))
    treeCache.getListenable.addListener(new TreeCacheListener() {
      @Override
      def childEvent(client: CuratorFramework, event: TreeCacheEvent) {
        val data = event.getData
        if (data != null) {
          event.getType match {
            case TreeCacheEvent.Type.NODE_ADDED =>
              add(data.getPath, new String(data.getData), data.getStat.getMtime)
              println("NODE_ADDED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case TreeCacheEvent.Type.NODE_REMOVED =>
              remove(data.getPath)
              println("NODE_REMOVED : " + data.getPath)
            case TreeCacheEvent.Type.NODE_UPDATED =>
              update(data.getPath, Array(new String(data.getData)), data.getStat.getMtime)
              println("NODE_UPDATED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case _ => println("event.getType=" + event.getType + " is not support")
          }
        } else {
          println("data is null : " + event.getType)
        }
      }
    })
    treeCache.start()

  }

  /**
    * 创建或更新节点值
    * 如果该节点存在则更新，否则创建并赋值
    * @param zkAddress
    * @param path
    * @param byte
    */
  def createAndSetData(zkAddress: String, path: String, byte: Array[Byte]): Unit = {
    if (!checkExist(zkAddress, path)) {
      getZkClient(zkAddress).create().creatingParentsIfNeeded().forPath(getPath(zkAddress, path), byte)
    } else {
      getZkClient(zkAddress).setData().forPath(getPath(zkAddress, path), byte)
    }
  }

  /**
    * 创建节点目录
    * @param zkAddress
    * @param path
    */
  def createPath(zkAddress: String, path: String): Unit = {
    if (!checkExist(zkAddress, path)) {
      getZkClient(zkAddress).create().creatingParentsIfNeeded().forPath(getPath(zkAddress, path))
    }
  }

  /**
    * 得到当前目录下的所有节点（仅限当前目录）
    * @param zkAddress
    * @param path
    * @return
    */
  def getChildren(zkAddress: String, path: String): Seq[String] = {
    val list = getZkClient(zkAddress).getChildren.forPath(getPath(zkAddress, path))
    import scala.collection.JavaConverters._
    list.asScala
  }

  /**
    * 给节点赋值
    * @param zkAddress
    * @param path
    * @param payload
    * @return
    */
  def setData(zkAddress: String, path: String, payload: Array[Byte]): Stat = {
    getZkClient(zkAddress).setData().forPath(getPath(zkAddress, path), payload)
  }

  /**
    * 得到该节点的值
    * @param zkAddress
    * @param path
    * @return
    */
  def getData(zkAddress: String, path: String): Array[Byte] = {
    getZkClient(zkAddress).getData.forPath(getPath(zkAddress, path))
  }

  /**
    * 删除目录、节点
    * @param zkAddress
    * @param path
    */
  def delete(zkAddress: String, path: String): Unit = {
    if (checkExist(zkAddress, path)) {
      getZkClient(zkAddress).delete().deletingChildrenIfNeeded().forPath(getPath(zkAddress, path))
    }
  }

  /**
    * 判断是否存在
    * @param zkAddress
    * @param path
    * @return
    */
  def checkExist(zkAddress: String, path: String): Boolean = {
    getZkClient(zkAddress).checkExists().forPath(getPath(zkAddress, path)) != null
  }

  /**
    * ip1/2181,ip2/2181,ip3/2181,ip4/2181 转换成 ip1,ip2,ip3,ip4
    * @param zkAddress
    * @return
    */
  def getZkAddress(zkAddress: String): String = zkAddress.split(",").map(_.split("\\/")(0)).mkString(",")

  def getPath(zkAddress: String, path: String): String = {
    val temp = zkAddress.split(",")(0).split("\\/").drop(1).mkString("/") + path
    if (temp.startsWith("/")) temp else "/" + temp
  }

}
