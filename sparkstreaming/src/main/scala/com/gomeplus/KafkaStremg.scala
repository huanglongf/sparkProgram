package com.gomeplus

import java.util.logging.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andy on 2018/4/2.
  */
object KafkaStremg {
  def main(args: Array[String]): Unit = {

    //日志日志级别
    Logger.getLogger("org").setLevel(Level.WARNING)
    val conf = new SparkConf().setAppName("KafkaStremg").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    



  }

}
