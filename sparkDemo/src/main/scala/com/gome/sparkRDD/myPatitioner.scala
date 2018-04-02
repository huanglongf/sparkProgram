package com.gome.sparkRDD

import org.apache.spark.Partitioner

/**
  * Created by Andy on 2018/4/2.
  */
class myPatitioner extends Partitioner {

  val map = Map("bigdata" -> 1, "java" -> 2, "php" -> 3)

  override def numPartitions: Int = map.size + 1

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Tuple2[String, String]]._1
    map.getOrElse(k, 0)
  }
}
