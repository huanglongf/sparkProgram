package com.gomeplus

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实时的输入字数统计
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //五秒拉取一次消息;
    val ssc = new StreamingContext(sc,Seconds(6))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("m0",8888)
    val lineToWords: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    lineToWords.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
