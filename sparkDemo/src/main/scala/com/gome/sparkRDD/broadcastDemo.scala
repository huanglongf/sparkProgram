package com.gome.sparkRDD

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量: Driver 将driver端的数据通过网络广播给属于自己认为的Excutor(进程,多个task,)
  * 避免了将Driver端 定义的变量发给每一个Task ,造成内存的浪费和网络IO 的额外开销
  */
object broadcastDemo {
  def main(args: Array[String]): Unit = {


    //设置本地运行;
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val rules: Map[String, String] = Map(("usa","美国"),("ch","中国"),("jp","日本"))

    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(rules)

    val result = sc.textFile(args(0)).map(line => {
      val split = line.split(" ")
      val name = split(0)
      val country = split(1)
      val value: Map[String, String] = broadcast.value
      val country_ch = value.getOrElse(country,null)
      (name, country_ch)
    })
    println(result.collect().toBuffer)

  }

}
