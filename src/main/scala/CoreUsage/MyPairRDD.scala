package CoreUsage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MyPairRDD {

  def myCombineByKey(sc: SparkContext){
    // we have string string RDD. first is key, second one is times appeared.
    val data = RddGenerator.GenerateStrStrRDD(sc)
    def create = (s: String) => s.toLong
    def merge = (l: Long, s: String) => l + s.toLong
    def combine = (l1: Long, l2: Long) => l1 + l2
    val resRDD = data.combineByKey[Long](create, merge, combine)
    resRDD.foreach(i => println(i._1 + " as key appeared " + i._2 + " times."))
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App")
    myCombineByKey(sc)

    sc.stop

  }
}
