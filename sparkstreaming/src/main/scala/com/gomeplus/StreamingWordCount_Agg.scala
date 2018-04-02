package com.gomeplus

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 实时的输入字数统计
  */
object StreamingWordCount_Agg {
  /*
  * 参数一: 是单词
  * 参数二: 是当前该单词出现的次数
  * 参数三: 初始值或者是以前累加过的值:
  * */
  val aggFunc =(it:Iterator[(String, Seq[Int], Option[Int])])=>{
    it.map(t=> (t._1,t._2.sum +t._3.getOrElse(0)))

  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //五秒拉取一次消息;
    val ssc = new StreamingContext(sc,Seconds(6))
   //如果想要跟新历史状态(需要),要要设置checkpoint
    ssc.checkpoint("./kfc")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("m0",8888)
    val lineToWords: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
    val result: DStream[(String, Int)] = lineToWords.updateStateByKey(aggFunc,new HashPartitioner(ssc.sparkContext.defaultMinPartitions),true)

    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
