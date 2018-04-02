package com.gome.sparkRDD

import scala.io.Source

/**
  * Created by Andy on 2018/3/27.
  */
object ClearData {

  def main(args: Array[String]): Unit = {
    val file = Source.fromFile(args(0))

    for (line <- file.getLines()) {

      val ls = line.split("as")(1).split(" ")
     //println(ls)

      val result =","+"SUM(NVL(a." +ls(1) +","+ "0"+"))" +" AS " + ls(1)

      println(result)
    }

    file.close()
  }

}
