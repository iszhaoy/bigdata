
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

import java.sql.Timestamp


case class A(nameA: String, tagA: String, Ats: Timestamp)

case class B(nameB: String, tagB: String, Bts: Timestamp)

object LeftJoinTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("LeftJoinTest")
      .getOrCreate()

    import spark.implicits._
    val aStream = spark.readStream.format("socket").option("host","localhost").option("port",9999).load.as[String]
    .as[String]
    .map(r => {
      val arrs = r.split(",")
      A(arrs(0).trim, arrs(1).trim, Timestamp.valueOf(arrs(2).trim))
    }).withWatermark("Ats", "1 seconds")

    val bStream = spark.readStream.format("socket").option("host","localhost").option("port",9998).load.as[String]
      .map(r => {
        val arrs = r.split(",")
        B(arrs(0).trim, arrs(1).trim, Timestamp.valueOf(arrs(2).trim))
      }).withWatermark("Bts", "1 seconds")

    //aStream.writeStream.format("console").start()
    //bStream.writeStream.format("console").start()

    val query = aStream.join(bStream,expr(
      s"""
         |nameA = nameB and
         |Bts >= Ats and Bts <= Ats + interval 10 seconds
         |""".stripMargin),"leftOuter")
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(2000))
      .outputMode(OutputMode.Append())
      .start();

    query.awaitTermination()

  }
}
