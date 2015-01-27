package MySql

import org.apache.spark.sql.catalyst.SqlParser

/**
 * Created by junius on 15-1-25.
 */
object MySqlParserUsage {
  def main (args: Array[String]) {
    val sp: SqlParser = new SqlParser
    val cmd = sp("SELECT COUNT(*) FROM records")
    println(cmd.toString)

    // 4、hive不支持INSERT INTO, UPDATE, DELETE操作
    val sp2: SqlParser = new SqlParser
    val cmd2 = sp2("INSERT INTO records VALUES(1,2)")
    println(cmd2.toString)
  }

}
