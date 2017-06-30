package com.hari.sparkhbase.hbase

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.{ Row, SQLContext }
import unicredit.spark.hbase._
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }

object readwritetoHbase {
  case class Schema(name: String, age: Int, gender: String)

  def main(args: Array[String]): Unit = {

    //    Setting spark context
    val conf = new SparkConf().setAppName("ExploreSparknHbase").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    import sqlContext.implicits._
    //    Read file to spark
    val data = sc.textFile("/tmp/test.dat")
    //    Define a schema with case class

    //    Skip header -> Split lines on delimiter -> Assign a schema to data
    val dataRDD = data.filter { line => !line.contains("Name,Age,Gender") }
      .map { line => line.split(",") }
      .map { x => Schema(x(0), x(1).toInt, x(2)) }
    val dataDF = dataRDD.toDF()
    dataDF.show()
    //Result so far   
    //+--------+---+------+
    //|    name|age|gender|
    //+--------+---+------+
    //|John Doe| 21|  Male|
    //|Jane Doe| 21|Female|
    //+--------+---+------+

    implicit val config = HBaseConfig()

    // Convert our dataframe to a Map() tuples/key value pair -since hbase needs key value pair
    dataDF.map { x =>
      val Array(col1, col2, col3) = x.toSeq.toArray
      val content = Map("name" -> col1.toString(), "age" -> col2.toString(), "gender" -> col3.toString())
      //      Make col1[Name] as Hbase row Key, in this case name is row key[not ideal but you get the gist ]
      //      And all other key value pair Map is passed as the fields into column family 1
      col1.toString() -> content
    }.toHBase(s"test", "cf1")

    //    Bulk method to multiple column families
    dataDF.map { x =>
      val Array(col1, col2, col3) = x.toSeq.toArray
      //      Making two set of Maps one for column family 1(cf1) and other for column family 2(cf2)
      val myCols1 = Map("name" -> col1.toString(), "age" -> col2.toString())
      val myCols2 = Map("gender" -> col3.toString())
      val content = Map("cf1" -> myCols1, "cf2" -> myCols2)
      col1.toString() -> content
    }.toHBaseBulk(s"test")

    //    Read from Hbase -entire data at once - assuming all our data is loaded in single column family

    //Making a map of our columns
    val columnlist = Map("cf1" -> Set("name", "age", "gender"))
    //Read data from hbase and assign into sparkSQL Row- This will create a RDD of Rows
    val hbaseRDD = sc.hbase[String](s"test", columnlist).map({
      case (k, v) =>
        val cf = v("cf1")
        Row(k, cf("name"), cf("age"), cf("gender"))
    })
    //Convert RDD to DF using a case class of our columns- you can type case the data if you need at this stage
    val hbaseDF = hbaseRDD.map({
      case Row(val1: String, val2: String, val3: String) => (val1, val2, val3)
    }).toDF("key", "name", "age", "gender")

  }

}