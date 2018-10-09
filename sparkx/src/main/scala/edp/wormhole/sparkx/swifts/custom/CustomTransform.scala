package edp.wormhole.sparkx.swifts.custom

import edp.wormhole.sparkx.batchflow.BatchflowMainProcess.logInfo
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsInterface, SwiftsProcessConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CustomTransform extends SwiftsInterface with EdpLogging {
  override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig): DataFrame = {
    println("df.schema.treeString println:" + df.schema.treeString + "aaaaaaaa")
    logInfo("df.schema.treeString info:" + df.schema.treeString + "aaaaaaaa")
    logWarning("df.schema.treeString warning:" + df.schema.treeString + "aaaaaaaa")
    logError("df.schema.treeString error:" + df.schema.treeString + "aaaaaaaa")
    df.select("id").where("id > 2")
  }

}
