package com.gome.sparkRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andy on 2018/3/27.
  */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JavaWordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val lineSplit: RDD[String] = sc.textFile(args(0)).flatMap(_.split(" "))
    lineSplit.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).saveAsTextFile(args(1))
    sc.stop()
  }

}
