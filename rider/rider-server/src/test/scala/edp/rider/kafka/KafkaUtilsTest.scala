package edp.rider.kafka

import org.scalatest.FunSuite

/**
  * Author: bjwangguangliang
  * Date: 2018/10/9
  * Description: 
  */
class KafkaUtilsTest extends FunSuite{
  test("test kafkaLatestOffset") {
    val brokers: String = "sparkmaster:9092"
    val topic: String = "flinkout"
    val kafkaLatestOffset =  KafkaUtils.getKafkaLatestOffset(brokers, topic)

    println(kafkaLatestOffset)
    //  0:3631813,1:44,2:2785445
    // 该方法与下面命令等价
    //# /usr/local/kafka_2.11-1.0.1/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list sparkmaster:9092 --topic flinkout --time -1
    // flinkout:2:2785445
    // flinkout:1:44
    // flinkout:0:3631813

  }

  test("test  kafkaLatestOffset partition") {
    val brokers: String = "sparkmaster:9092"
    val topic: String = "flinkout"
    val kafkaLatestOffset =  KafkaUtils.getKafkaLatestOffset(brokers, topic, 1)
    println(kafkaLatestOffset)   // 44
    // 该方法与下面命令等价
    //# /usr/local/kafka_2.11-1.0.1/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list sparkmaster:9092 --topic flinkout --time -1 --partitions 1
    // flinkout:1:44


  }


  test("test kafkaEarliestOffset") {
    val brokers: String = "sparkmaster:9092"
    val topic: String = "flinkout"
    val kafkaEarliestOffset =  KafkaUtils.getKafkaEarliestOffset(brokers, topic)
    println(kafkaEarliestOffset)
    // 0:3631809,1:41,2:2785442
    // 该方法与下面命令等价
    //# /usr/local/kafka_2.11-1.0.1/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list sparkmaster:9092 --topic flinkout --time -2
    // flinkout:2:2785442
    // flinkout:1:41
    // flinkout:0:3631809

  }

  test("test") {
    val brokers: String = "sparkmaster:9092"
    val topic: String = "flinkout"
    KafkaUtils
  }






}
