package edp.wormhole.dbdriver.dbpool

import edp.wormhole.util.config.{ConnectionConfig, KVConfig}

object DbConnectionTest {
  def main(args:Array[String]) = {
    val url = "jdbc:mysql://sparkmaster:3306/wormhole?useUnicode=true&characterEncoding=UTF-8&useSSL=false"
    val username = "wormhole"
    val password = "wormhole"
    val parameters = Seq[KVConfig]()
    parameters .+: (KVConfig("cachePrepStmts", "true"))
    parameters .+: (KVConfig("prepStmtCacheSize", "250"))
    val connectionConfig = ConnectionConfig(url, Option(username), Option(password), Option(parameters))
    // 1)创建dataSource从中取得一个Connection
    val connection = DbConnection.getConnection(connectionConfig)
    println(connection.getCatalog)

    val statement = connection.prepareStatement("select * from user")
    val result = statement.executeQuery()
    val metaData = result.getMetaData
    val columCount = metaData.getColumnCount
    for(idx <- 1.to(columCount)) {
      print(metaData.getColumnName(idx)+"\t")
    }
    println
    while (result.next()) {

      for(idx <- 1.to(columCount)) {
        print(result.getObject(idx)+"\t")
      }
      println

    }

    // 2)关闭dataSource
    println("关闭前："+DbConnection.datasourceMap)
    DbConnection.shutdownConnection(url, username)
    println("关闭后："+DbConnection.datasourceMap)


  }



}
