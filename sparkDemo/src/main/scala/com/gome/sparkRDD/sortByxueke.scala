package com.gome.sparkRDD

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andy on 2018/3/30.
  */
object sortByxueke {
  def main(args: Array[String]): Unit = {
    //设置本地运行;
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local")
    val sc = new SparkContext(conf)


    val index: RDD[(String, String)] = sc.textFile(args(0)).map(line => {
      val url = new URL(line)
      val xueke = url.getHost.split("\\.")(0)
      val name = url.getPath.substring(1)
      (xueke, name)
    })


   /* //方式一 聚合后数据量级不是很大 ----将排序的值固定;
    //将shuffle 和repatitioner 的步骤和并 减少一次shuffle;
    val reduAndpat: RDD[((String, String), Int)] = index.map((_, 1)).reduceByKey(new myPatitioner, _ + _)
    //分区函数:
    val result = reduAndpat.mapPartitions(t => t.toList.sortBy(_._2).reverse.take(2).iterator)
    println(result.collect().toBuffer)*/


    //方式二: 应用场景  针对数据量比较大的情况  不做toList 转换; 用RDD的排序方法

    // 从MySQL 中获取数据
    val lst = List("bigdata","java","php")
    for(i <-lst){
      val sum_index: RDD[((String, String), Int)] = index.filter(x=>i.equals(x._1)).map((_, 1)).reduceByKey(_+_)
      //RDD 上面排序
      val result = sum_index.sortBy(_._2,false).take(2)
      println(result.toBuffer)
    }

    sc.stop()

  }

}
