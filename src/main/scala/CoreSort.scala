/**
 * Created by junius on 14-8-18.
 */

// package name for deployed via this package.
package org.apache.spark.examples

import java.util.Random
import java.net.InetAddress
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.Array.canBuildFrom
import scala.collection.mutable.ListBuffer


//this one successfully run at spark cluster.
object CoreSort {
    def SortOnPreferredLocation(sc: SparkContext): Unit ={
    val sc = new SparkContext("spark://172.16.32.1:7077", "CoreSort")

    val random = new Random()
    val dataLen = 1000
    //generate so many integer
    val dataSet: Array[Int] = new Array[Int](dataLen)
    (0 until dataLen).map(index => (dataSet(index) = random.nextInt()))

    val dataRdd = sc.makeRDD(dataSet, 10)
    dataRdd.map(i => (i, i)).sortByKey(true, 1).map(println).count
  }
}


