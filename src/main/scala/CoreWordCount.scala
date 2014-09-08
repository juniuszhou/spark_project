/**
 * Created by junius on 14-8-18.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.Array.canBuildFrom


object CoreWordCount{
  def main(args: Array[String]) {
    //get rdd from local file
    //val logFile = "/home/junius/git_hub/spark_project/resource/number.txt" // Should be some file on your system

    //get rdd from hdfs.
    val logFile = "hdfs://172.16.32.1:9000/input/number.txt"

    //val sc = new SparkContext("local[4]", "Simple App")
    //ip as virtual one assigned from vm station.
    val sc = new SparkContext("spark://172.16.32.1:7077", "Simple App")

    //if use ubuntu, doesn't work. reason is unclear.
    //val sc = new SparkContext("spark://ubuntu:7077", "Simple App")

    //cache rdd to memory
    val logData = sc.textFile(logFile, 2).cache().count()
    print("junius get")
  }
}


