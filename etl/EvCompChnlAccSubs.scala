package com.att.eview.etl

import java.io.File

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory

object EvCompChnlAccSubs extends SparkSessionWrapper with PartitionCleanup {

  var ev_accnt_pack_mar_df         = spark.emptyDataFrame
  var ev_accnt_subs_prev_df        = spark.emptyDataFrame
  var ev_accnt_pack_mar_maxdt: Int = 0
  var noOfPartFiles: Int           = 0

  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
  val utcTimestamp   = ZonedDateTime.now.withZoneSameInstant(ZoneId.of("UTC")).format(dateTimeFormat)

  def createTable(CONF_EV_CD_HIVE_DB: String, CONF_EV_CD_HIVE_TABLE_LOC: String): Unit = {
    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS $CONF_EV_CD_HIVE_DB.EV_COMP_CHNL_ACCOUNT_SUBSCRIPTION(
          DTV_BAN STRING,
          BK_PKG_TYPE STRING,
          DTV_PREFIX_CD STRING,
          DTV_SERVICE_CD INT,
          BK_PKG_BLNG_CD STRING,
          PACKAGE_NAME  STRING,
          STATUS_CD INT COMMENT '1-Active,0-Inactive',
          UPD_IND INT COMMENT '0-No Change,1-Updated',
          LOAD_TS TIMESTAMP,
          SRC_DATA_DT INT)
          PARTITIONED BY(DATA_DT INT)
          STORED AS ORC
          LOCATION '$CONF_EV_CD_HIVE_TABLE_LOC/EV_COMP_CHNL_ACCOUNT_SUBSCRIPTION'
          TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY")
          """.stripMargin
    )
  }

  def initDataFrames(
      CONF_EV_CD_HIVE_DB: String,
      CONF_EV_MD_HIVE_DB: String,
      EV_DATA_DT: Int,
      EV_DATA_LOAD_DT: Int): Unit = {
    import spark.implicits._
    ev_accnt_pack_mar_maxdt = spark
      .sql(s"show partitions $CONF_EV_MD_HIVE_DB.EV_ACCOUNT_PACKAGE_MARKET partition(business_line='DIRECTV')")
      .select($"partition")
      .map(A => A.toString().substring(9, 17).toInt)
      .filter(a => (a <= EV_DATA_LOAD_DT))
      .toDF()
      .sort(desc("value"))
      .head
      .getInt(0)

    ev_accnt_pack_mar_df = spark
      .table(s"$CONF_EV_MD_HIVE_DB.EV_ACCOUNT_PACKAGE_MARKET")
      .filter($"data_dt" === ev_accnt_pack_mar_maxdt && $"business_line" === "DIRECTV" && $"src_system" === "STMS")
      .select($"ban",
              $"package_type",
              $"dtv_svc_prefix_cd",
              $"dtv_svc_cd",
              $"package_billing_cd",
              $"package_name",
              $"data_dt")
      .withColumn(
        s"apm_rank",
        row_number()
          .over(
            Window
              .partitionBy("ban", "dtv_svc_prefix_cd", "dtv_svc_cd", "package_billing_cd")
              .orderBy(col("ban").desc))
      )
      .filter("apm_rank=1")

    ev_accnt_subs_prev_df = spark
      .table(s" $CONF_EV_CD_HIVE_DB.EV_COMP_CHNL_ACCOUNT_SUBSCRIPTION")
      .filter($"DATA_DT" === EV_DATA_DT && $"STATUS_CD" === 1)
      .select(
        $"dtv_ban",
        $"bk_pkg_type",
        $"dtv_prefix_cd",
        $"dtv_service_cd",
        $"bk_pkg_blng_cd",
        $"package_name",
        $"src_data_dt",
        concat($"dtv_ban", $"dtv_prefix_cd", $"dtv_service_cd", $"bk_pkg_blng_cd").alias("tgt_key")
      )

  }

  def initialLoad(CONF_EV_CD_HIVE_DB: String): Unit = {

    val tmp_df = ev_accnt_pack_mar_df
      .withColumn("status_cd", lit(1))
      .withColumn("upd_ind", lit(1))
      .withColumn("load_ts", lit(utcTimestamp))

    val finaldata = tmp_df.select("ban",
                                  "package_type",
                                  "dtv_svc_prefix_cd",
                                  "dtv_svc_cd",
                                  "package_billing_cd",
                                  "package_name",
                                  "status_cd",
                                  "upd_ind",
                                  "load_ts",
                                  "data_dt")

    writeTablePartition(CONF_EV_CD_HIVE_DB, ev_accnt_pack_mar_maxdt, finaldata, noOfPartFiles)
  }

  def deltaLoad(CONF_EV_CD_HIVE_DB: String, partitionDate: Int): Unit = {

    val tmp_df = ev_accnt_pack_mar_df.withColumn(
      "src_key",
      concat(col("ban"), col("dtv_svc_prefix_cd"), col("dtv_svc_cd"), col("package_billing_cd")))

    val joined_src_tgt = ev_accnt_subs_prev_df
      .as("acct_subs_df")
      .join(tmp_df.as("apm_df"), ev_accnt_subs_prev_df.col("tgt_key") === tmp_df.col("src_key"), "fullouter")
      .select(
        col("acct_subs_df.tgt_key"),
        col("apm_df.src_key"),
        coalesce(col("acct_subs_df.dtv_ban"), col("apm_df.ban")).alias("dtv_ban"),
        coalesce(col("acct_subs_df.dtv_prefix_cd"), col("apm_df.dtv_svc_prefix_cd")).alias("dtv_prefix_cd"),
        coalesce(col("acct_subs_df.dtv_service_cd"), col("apm_df.dtv_svc_cd")).alias("dtv_service_cd"),
        coalesce(col("acct_subs_df.bk_pkg_blng_cd"), col("apm_df.package_billing_cd")).alias("bk_pkg_blng_cd"),
        when(col("apm_df.src_key").isNotNull, col("apm_df.package_type"))
          .otherwise(col("acct_subs_df.bk_pkg_type"))
          .alias("bk_pkg_type"),
        when(col("apm_df.src_key").isNotNull, col("apm_df.package_name"))
          .otherwise(col("acct_subs_df.package_name"))
          .alias("package_name"),
        when(col("acct_subs_df.tgt_key").isNull && col("apm_df.src_key").isNotNull, 1)
          .when(col("acct_subs_df.tgt_key").isNotNull && col("apm_df.src_key").isNull, 0)
          .otherwise(1)
          .alias("status_cd"),
        when(col("acct_subs_df.tgt_key").isNull && col("apm_df.src_key").isNotNull, 1)
          .when(col("acct_subs_df.tgt_key").isNotNull && col("apm_df.src_key").isNull, 1)
          .otherwise(0)
          .alias("upd_ind"),
        when(col("apm_df.data_dt").isNotNull && col("acct_subs_df.tgt_key").isNull && col("apm_df.src_key").isNotNull,
             col("apm_df.data_dt"))
          .when(
            col("apm_df.data_dt").isNotNull && col("acct_subs_df.tgt_key").isNotNull && col("apm_df.src_key").isNull,
            ev_accnt_pack_mar_maxdt.toString)
          .otherwise(col("acct_subs_df.src_data_dt"))
          .alias("data_dt")
      )
      .withColumn("load_ts", lit(utcTimestamp))

    val finaldata = joined_src_tgt.select("dtv_ban",
                                          "bk_pkg_type",
                                          "dtv_prefix_cd",
                                          "dtv_service_cd",
                                          "bk_pkg_blng_cd",
                                          "package_name",
                                          "status_cd",
                                          "upd_ind",
                                          "load_ts",
                                          "data_dt")
    writeTablePartition(CONF_EV_CD_HIVE_DB, partitionDate, finaldata, noOfPartFiles)
  }

  def writeTablePartition(
      CONF_EV_CD_HIVE_DB: String,
      partitionDate: Int,
      finalData: DataFrame,
      noOfPartFiles: Int): Unit = {

    finalData.coalesce(noOfPartFiles).createOrReplaceTempView("finalData")

    spark.sql(s"""
                 | Insert Overwrite Table $CONF_EV_CD_HIVE_DB.EV_COMP_CHNL_ACCOUNT_SUBSCRIPTION Partition(data_dt=$partitionDate)
                 | Select * from finalData
  """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val exec_env          = args(0) // Load type Initial or Incremental Load
    val loadType          = args(1) // Load type Initial or Incremental Load
    val prevpartitionDate = args(2).toInt
    val partitionDate     = args(3).toInt

    /* Default System Properties */
    val config = ConfigFactory
      .parseFile(new File("application.conf"))
      .withFallback(ConfigFactory.load())
      .resolve
      .getConfig(exec_env)

    val CONF_EV_CD_HIVE_DB        = config.getString("CONF_EV_CD_HIVE_DB")
    val CONF_EV_CD_HIVE_TABLE_LOC = config.getString("CONF_EV_CD_HIVE_TABLE_LOC")
    val CONF_EV_MD_HIVE_DB        = config.getString("CONF_EV_MD_HIVE_DB")
    val RETENTION_DAYS            = config.getString("RETENTION_DAYS")
    noOfPartFiles = config.getString("AccSubsFileCnt").toInt

    def load(option: String) = option.toUpperCase() match {
      case "CREATE" => createTable(CONF_EV_CD_HIVE_DB, CONF_EV_CD_HIVE_TABLE_LOC)
      case "INITIAL" =>
        createTable(CONF_EV_CD_HIVE_DB, CONF_EV_CD_HIVE_TABLE_LOC)
        initDataFrames(CONF_EV_CD_HIVE_DB, CONF_EV_MD_HIVE_DB, prevpartitionDate, partitionDate)
        initialLoad(CONF_EV_CD_HIVE_DB)
      case "DELTA" =>
        initDataFrames(CONF_EV_CD_HIVE_DB, CONF_EV_MD_HIVE_DB, prevpartitionDate, partitionDate)
        deltaLoad(CONF_EV_CD_HIVE_DB, partitionDate)
        partitionPurge(CONF_EV_CD_HIVE_DB,
                       CONF_EV_CD_HIVE_TABLE_LOC,
                       "EV_COMP_CHNL_ACCOUNT_SUBSCRIPTION",
                       "data_dt",
                       RETENTION_DAYS.toInt)
      case _ => throw new IllegalArgumentException("Invalid Load Type Passed")
    }
    load(loadType)

  }
}
