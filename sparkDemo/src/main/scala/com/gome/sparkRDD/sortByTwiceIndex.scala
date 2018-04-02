package com.gome.sparkRDD

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ArrayBuffer(
  * (yuwen,
  * List(((yuwen,lili),2), ((yuwen,ali),1), ((yuwen,sasha),1))),
  * (shuxue,
  * List(((shuxue,laohuang),3), ((shuxue,zhangsan),3), ((shuxue,meimei),3), ((shuxue,tingting),1), ((shuxue,lili),1))),
  * (english,
  * List(((english,guoguo),2), ((english,qingwen),2), ((english,meimei),1), ((english,zhangsan),1), ((english,lili),1))))

.
  */
object sortByTwiceIndex {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sortByTwiceIndex").setMaster("local")
    val sc = new SparkContext(conf)

    //
    val line = sc.textFile(args(0))

    val lineToTuole: RDD[(String, String)] = line.map(line => {
      val url = new URL(line)
      val host = url.getHost
      val path = url.getPath
      val xueke = host.split("\\.")(0)
      val name = path.substring(1)
      (xueke, name)
    })

    val keyRududc: RDD[((String, String), Int)] = lineToTuole.map((_,1)).reduceByKey(_+_)

    //按照xueke分组===========================================================================
    val groupByXueKe: RDD[(String, Iterable[((String, String), Int)])] = keyRududc.groupBy(_._1._1)
    val orderByInter = groupByXueKe.mapValues(_.toList.sortBy(_._2).reverse)
    orderByInter.saveAsTextFile(args(1))
    println(orderByInter.collect.toBuffer)
  }
}
