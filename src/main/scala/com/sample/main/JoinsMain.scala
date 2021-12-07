package com.sample.main


import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, desc, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object JoinsMain {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkExample")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate
    import spark.implicits._
    val accountsLoc = "/user/training/surender_hadoop/input_files/accounts_profile/accounts_profile.txt"

    val atmTransLoc = "/user/training/surender_hadoop/input_files/atm_trans/atm_trans.txt"

    val bName = args(0)

    val accountsSchema = StructType(Array(
      StructField("account_no", StringType, true),
      StructField("bank_name", StringType, true),
      StructField("cust_name", StringType, true),
      StructField("gender", StringType, true),
      StructField("ph_no", StringType, true)
    ))


    val atmTransSchema = StructType(Array(
      StructField("account_id", StringType, true),
      StructField("atm_id", StringType, true),
      StructField("trans_dt", StringType, true),
      StructField("amount", StringType, true),
      StructField("status", StringType, true)
    ))

    val accountsRDD = spark.sparkContext.textFile(accountsLoc)

    val transRDD = spark.sparkContext.textFile(atmTransLoc)

    val accountsRowRDD = accountsRDD.map(rec => rec.split(",")).map(arr => org.apache.spark.sql.Row(arr:_*))

    val transRowRDD = transRDD.map(rec => rec.split("~")).map(arr => org.apache.spark.sql.Row(arr:_*))

    val accountsDF = spark.createDataFrame(accountsRowRDD,accountsSchema)

    val transDF = spark.createDataFrame(transRowRDD,atmTransSchema)

    val innerJoinedDF = accountsDF.join(transDF, accountsDF("account_no") ===  transDF("account_id") , "inner" )

    innerJoinedDF.createOrReplaceTempView("t1")

    spark.sql(
      s"""
        |select * from t1 where bank_name = '${bName}'
        |""".stripMargin).show(100,false)

    val leftJoinedDF = accountsDF.join(transDF, accountsDF("account_no") ===  transDF("account_id") , "left_outer" )

    //leftJoinedDF.filter(leftJoinedDF("atm_id").isNull).select("account_no","bank_name","cust_name","gender","ph_no").show

    val leftAntiJoinedDF = accountsDF.join(transDF, accountsDF("account_no") ===  transDF("account_id") , "left_anti" )
  }

}
