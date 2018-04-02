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

    //对算指标获取总的统计
    val Index_count: RDD[((String, String), Int)] = line_index.map((_, 1)).reduceByKey(_ + _)

    //Index_count 的数据后面是重复使用的 所以做一下cache 做一下优化; cache的提 是shuff
    Index_count.cache()

    //统计key -- 统计xueke 的个数确定分几个分区=====注意action一下 : 这一步的计算任务可以从数据库中查询替换掉;(这是优化的一个步骤的: )
    val xueke_distinct: Array[String] = Index_count.map(_._1._1).distinct().collect()

    //提取key进行分区的操作;partitionBy 本身就是action 的操作;
    val index_partition: RDD[(String, ((String, String), Int))] = Index_count.map(t => (t._1._1, (t._1, t._2))).partitionBy(new MyParttitioner(xueke_distinct))

    //获取分区并且对分区内部排序获取topN  ===优化 使用list 的排序的话 数据量非常大 容易outofmember ===> 转换成RDD 的排序方式  并规排序 部分数据会落实到吸盘;
    val result: RDD[(String, ((String, String), Int))] = index_partition.mapPartitions(x => x.toList.sortBy(x => x._2._2).reverse.take(2).iterator)
    result.saveAsTextFile(args(1))
    //释放资源
    sc.stop()


  }
}

class MyParttitioner(arr: Array[String]) extends Partitioner {

  //将xueke 放入到map中去 ===注意map 的类型万千不能够写成Integer了;
  private val map = new mutable.HashMap[String, Int]()
  var num = 0
  for (i <- arr) {
    map.put(i, num)
    num += 1
  }

  override def numPartitions: Int = arr.length

  override def getPartition(key: Any): Int = {
    val k = key.toString
    map.getOrElse(k, 0)
  }
}