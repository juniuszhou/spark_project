package MySql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object MyJason {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)
    val path = "/home/junius/my_git/spark_1.1.0_comments/examples/src/main/resources/people.json"

    val people = sqlContext.jsonFile(path)

    people.printSchema()
    people.registerTempTable("people")
    // to keep table in cache and accelerate its read and write.
    sqlContext.cacheTable("people")

    // other method to create json RDD
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
    anotherPeople.map(println).count()
    sc.stop

  }
}
