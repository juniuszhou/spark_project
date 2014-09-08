/**
 * Created by junius on 14-8-18.
 */

// package name for deployed via this package.
package org.apache.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.Array.canBuildFrom


//this one successfully run at spark cluster.
object CoreRddPrint {
  def main(args: Array[String]) {
    println("junius start")
    val logFile = "hdfs://172.16.32.1:9000/input/number.txt"
    val sc = new SparkContext("spark://172.16.32.1:7077", "CoreRddPrint")

    val logData = sc.textFile(logFile, 2).cache().map(str => println(str + " junius done")).count()
    println("junius done")
  }
}


