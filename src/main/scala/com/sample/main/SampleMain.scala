package com.sample.main

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, desc, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object SampleMain {
case class AccountsProfile(account_no:String,
                           bank_name:String,
                           cust_name:String,
                           gender:String,
                           ph_no:String)
  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger(this.getClass)
   val spark = SparkSession.builder
      .appName("SparkExample")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate
    import spark.implicits._
    val loc = "/user/training/surender_hadoop/input_files/accounts_profile/accounts_profile.txt"
    log.info("Starting the spark application ....")
    val rdd = spark.sparkContext.textFile(loc)
    val schema = StructType(Array(
      StructField("account_no", StringType, true),
      StructField("bank_name", StringType, true),
      StructField("cust_name", StringType, true),
      StructField("gender", StringType, true),
      StructField("ph_no", StringType, true)
    ))

val rowRDD = rdd.map(rec => rec.split(",")).map(arr => org.apache.spark.sql.Row(arr:_*))

val caseDF = rdd.map(rec => rec.split(",")).map(arr => AccountsProfile(arr(0),arr(1),arr(2),arr(3),arr(4))).toDF()

val df = spark.createDataFrame(rowRDD,schema)
    df.printSchema()
    //df.show(100,false)
    df.show(11)
    val outputLoc = "/user/training/surender_hadoop/output_files/accounts_profile"
    //df.coalesce(1).write.format(format).mode(mode).save(hdfsOutputLoc)

    val selectedDF = df.select("bank_name","account_no", "gender")

    val sDF = df.select("bank_name","account_no")
    val df1 = df.drop("bank_name")
    df.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t2")
    val sqlDF = spark.sql(
      """
        |select account_no, ph_no, bank_name, 'INDIA' as country from t1 where bank_name = 'HDFC'
        |""".stripMargin
    )

    val colsList = List("account_no","ph_no","bank_name")
  val changedDF = df.withColumn("country",lit("INDIA"))
    .withColumn("c2",lit("HELLO"))
    .drop("c2")

    val mappedDF = df.select(colsList.map(m=>col(m)):_*).show()
    df.write.format("parquet").mode(SaveMode.Overwrite).save(outputLoc)

    val renamedDF = df.withColumnRenamed("ph_no", "mobile_no")

    val plainDF = spark
      .read
      .option("delimiter",",")
      .schema(schema)
      .csv(loc)

    val bankCountDF = spark.sql(
      """
        |select bank_name,count(*) as mycount from t2
        |where bank_name  not in ('CITI','HDFC')
        |group by bank_name
        |
        |""".stripMargin)

    val resultSQl = spark.sql(
      """
        |select bank_name,count(*) as cust_count from
        |(select account_no, ph_no, bank_name from
        |(select * from t2)a
        |where bank_name != 'HDFC')b
        |group by bank_name
        |having cust_count > 4
        |""".stripMargin
      )

  }



}
