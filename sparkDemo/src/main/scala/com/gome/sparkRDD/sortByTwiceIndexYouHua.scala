package com.gome.sparkRDD

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}
import scala.collection.mutable

/**
  * ArrayBuffer(
  * (yuwen,
  * List(((yuwen,lili),2), ((yuwen,ali),1), ((yuwen,sasha),1))),
  * (shuxue,
  * List(((shuxue,laohuang),3), ((shuxue,zhangsan),3), ((shuxue,meimei),3), ((shuxue,tingting),1), ((shuxue,lili),1))),
  * (english,
  * List(((english,guoguo),2), ((english,qingwen),2), ((english,meimei),1), ((english,zhangsan),1), ((english,lili),1))))
  * *
  * .
  * 分区器的使用  使用场景一 将不同key分到不同的分区:
  */
object sortByTwiceIndexWithParttitioner {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sortByTwiceIndex").setMaster("local")
    val sc = new SparkContext(conf)


    //获取计算指标
    val line_index: RDD[(String, String)] = sc.textFile(args(0)).map(line => {
      val url = new URL(line)
      val host = url.getHost
      val path = url.getPath
      val xueke = host.split("\\.")(0)
      val name = path.substring(1)
      (xueke, name)
    })

   /* 方案一
   1.使用的场景是 针对shuffle过后的数据/过滤 ---数据量不大的数据进行Driver端的tolist排序的方式:
   将科目写死(或者从数据从获取数据,减少count的shuffle */



  }
}

class MyParttitioner(arr: Array[String]) extends Partitioner {

  //将xueke 放入到map中去 ===注意map 的类型万千不能够写成Integer了;
  Val map1 = Map("bigdata"->1,"java"->2,"php"->3)

  override def numPartitions: Int = arr.length

  override def getPartition(key: Any): Int = {
    val k = key.toString
  }
}