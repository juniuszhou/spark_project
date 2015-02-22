package main.scala.MySql

import MySql.TableGenerator
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SchemaRDD, catalyst, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Created by junius on 15-2-2.
 */
object AnalyzeSql {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)
    val catalog: Catalog = new SimpleCatalog(true)
    TableGenerator.GenerateTable(sc, catalog)


    val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry
    val analyzer = new Analyzer(catalog, functionRegistry, caseSensitive = true)
    val parser = new catalyst.SqlParser
    val sqlText = "SELECT * FROM MyRecords"
    val plan: LogicalPlan = parser(sqlText)


    // we can't print schema here since the plan is not resolved yet.
    // plan.printSchema()

    println("++++++++++++++++++++++++++++++++++++")
    // get schema rdd from logical plan.
    val schemaRDD = new SchemaRDD(sqlContext, plan)

    println(schemaRDD.toString())

    // analyze logical plan. then get the syntax tree.
    val tree: TreeNode[_] = analyzer(plan)
    // println(tree.toString())
    plan.printSchema()



    // sqlContext.sql("SELECT * FROM MyRecords").collect.foreach(println)

  }
}
