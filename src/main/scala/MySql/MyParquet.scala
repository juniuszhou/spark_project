package MySql

import CoreUsage.RddGenerator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random


/**
 * Created by junius on 15-1-22.
 */
case class MyParquetRecord(id: Int, age: Int)
object MyParquet {
  // seems there is connection between case class and Product.
  //here path is a directory of Parquet.
  def SaveRDDAsParquet[A <: Product: TypeTag](rdd: RDD[A], path: String, sqlContext: SQLContext) = {
    import sqlContext.createSchemaRDD
    rdd.saveAsParquetFile(path)
  }
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)

    val r = new Random()

    val data = (0 to 10).map(i => MyParquetRecord(r.nextInt, r.nextInt))
    val rdd = sqlContext.sparkContext.parallelize(data)

    SaveRDDAsParquet(rdd, "/home/junius/stu.txt", sqlContext)
    sc.stop()
  }
}
