package edp.wormhole.externalclient.zookeeper

/**
  * 监听zk sparkmaster:2181，目录 /wormhole
  *
  */
object WormholeZkClientTest {
  def add(path: String, data: String, time: Long): Unit = {
    println("watch path:" + path)
    println("add data:"+data)
    println("time:"+time)
  }


  def remove(path: String): Unit = {
    println("delete path:" + path)
  }

  def update(path: String, data: Array[String], time: Long): Unit = {
    println("update path:" + path)
    println("update data:"+data.mkString(","))
    println("time:"+time)
  }
  def main(args:Array[String] ) = {
    WormholeZkClient.setTreeCacheListener("sparkmaster:2181", "/wormhole", add, remove, update)
    Thread.sleep(Integer.MAX_VALUE)
  }

}
