import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by junius on 14-8-17.
 */
object OneScala {

  def removeClouse(sc: SparkContext, data: RDD[String]){
    var sum = 0
    data.map(item =>  { println(item)
      sum += 1
      println(sum)
    }).count()
    println(sum)
  }

  def main(args: Array[String]){

    val nameList = List("junius", "zhou")
    nameList.map(println)

    println("hello world")
  }
}
