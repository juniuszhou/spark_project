package GraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
/**
 * Created by junius on 14-12-22.
 */
object LoadGraph {
  def main (args: Array[String]) {
    val path = "/home/junius/my_git/spark_1.1.0_comments/graphx/data/followers.txt"
    val sc = new SparkContext("local[4]", "Simple App")
    val g = GraphLoader.edgeListFile(sc, path, minEdgePartitions=2)
    println(g.toString)

  }

}
