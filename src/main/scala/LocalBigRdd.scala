/**
 * Created by junius on 14-8-18.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.Array.canBuildFrom

import java.util.Random

object LocalBigRdd {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App")

    val random = new Random()
    val dataLen = 1000
    //generate so many integer
    val dataSet: Array[Int] = new Array[Int](dataLen)
    (0 until dataLen).map(index => (dataSet(index) = random.nextInt()))

    val dataRdd = sc.makeRDD(dataSet, 10).cache
    println(dataRdd.max)
    println(dataRdd.min)

    val orderRdd = sc.makeRDD(dataSet, 10).cache.map(i => (i, null)).sortByKey().map(println).count()
  }
}

