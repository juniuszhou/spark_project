package CoreUsage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MyPairRDD {

  def myCombineByKey(sc: SparkContext){
    val data = RddGenerator.GeneratePairRDD(sc)
    val log2 = data.combineByKey[(Long, String)](
      //map value to tuple
      createCombiner = (s: String) => (1L, s),
      //merge init tuple and each value
      mergeValue = (c: (Long, String), v: String) => (c._1 + 2L, c._2 + v),
      //combine all partition of merger.
      mergeCombiners = (c1: (Long, String), c2: (Long, String)) => (c1._1 + c2._1, c1._2 + c2._2))

    log2.map(item => println(item._1 + " ij " + item._2._1 + " ji " + item._2._2)).count
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App")
    myCombineByKey(sc);

    sc.stop

  }
}
