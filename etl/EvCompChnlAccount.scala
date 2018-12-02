package com.att.eview.etl

import java.io.File

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.util.Calendar

import com.typesafe.config.ConfigFactory

object EvCompChnlAccount extends SparkSessionWrapper with PartitionCleanup {

  var ev_acc_df          = spark.emptyDataFrame
  var ev_dlr_df          = spark.emptyDataFrame
  var ev_cc_acc_df       = spark.emptyDataFrame
  var ev_acc_dlr_df      = spark.emptyDataFrame
  var accMaxDt: Int      = 0
  var noOfPartFiles: Int = 0

  def createTable(CONF_EV_CD_HIVE_DB: String, CONF_EV_CD_HIVE_TABLE_LOC: String): Unit = {
    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS $CONF_EV_CD_HIVE_DB.EV_COMP_CHNL_ACCOUNT(
          DTV_BAN STRING,
          VIDEO_SVC_STATUS STRING,
          SERVICE_ZIP_CD STRING,
          INIT_VIDEO_SVC_ESTB_DT TIMESTAMP,
          PARTNER_NAME STRING,
          DEALER_NAME STRING,
          DTV_DMA_CD STRING,
          UPD_IND INT COMMENT '0-No Change,1-Updated',
          LOAD_TS TIMESTAMP,
          SRC_DATA_DT INT)
          PARTITIONED BY(DATA_DT INT)
          STORED AS ORC
          LOCATION '$CONF_EV_CD_HIVE_TABLE_LOC/EV_COMP_CHNL_ACCOUNT'
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
    //accMaxDt = spark.sql(s"show partitions $CONF_EV_MD_HIVE_DB.ev_account partition(business_line='DIRECTV')").sort(desc("partition")).first.getString(0).split("/")(0).split("=")(1)
    //accMaxDt = spark.sql("show partitions eview_gold.ev_account").withColumn("tmp",split($"partition", "\\/")).withColumn("tmp1",$"tmp".getItem(0)).agg(max($"tmp1")).first.getString(0).split("=")(1)
    accMaxDt = spark
      .sql(s"show partitions $CONF_EV_MD_HIVE_DB.ev_account partition(business_line='DIRECTV')")
      .collect()
      .map(A => A.toString().substring(9, 17))
      .filter(a => (a <= EV_DATA_LOAD_DT.toString()))
      .sortWith(_ > _)
      .head
      .toInt

    //val dlrMaxDt = spark.sql(s"show partitions $CONF_EV_MD_HIVE_DB.ev_dealer partition(business_line='DIRECTV')").sort(desc("partition")).first.getString(0).split("/")(0).split("=")(1)
    val dlrMaxDt = spark
      .sql(s"show partitions $CONF_EV_MD_HIVE_DB.ev_dealer partition(business_line='DIRECTV')")
      .collect()
      .map(A => A.toString().substring(9, 17))
      .filter(a => (a <= EV_DATA_LOAD_DT.toString()))
      .sortWith(_ > _)
      .head
      .toInt

    ev_cc_acc_df = spark
      .table(s"$CONF_EV_CD_HIVE_DB.ev_comp_chnl_account")
      .filter($"DATA_DT" === EV_DATA_DT)
      .withColumn("attributes",
                  concat_ws(",",
                            $"video_svc_status",
                            $"service_zip_cd",
                            $"init_video_svc_estb_dt",
                            $"partner_name",
                            $"dealer_name",
                            $"dtv_dma_cd"))

    //Creation of ev_acc data frame with only selected columns
    ev_acc_df = spark
      .table(s"$CONF_EV_MD_HIVE_DB.ev_account")
      .filter($"data_dt" === accMaxDt && $"business_line" === "DIRECTV" && $"src_system" === "STMS")
      .select(
        "ban",
        "account_start_dt",
        "cust_acct_stat_desc",
        "last_disc_rsn_cd",
        "zip",
        "account_start_dt",
        "dtv_dma_cd",
        "dlr_acct_key",
        "status",
        "src_system",
        "data_dt"
      )
      .withColumn(s"ban_rank", row_number().over(Window.partitionBy("ban").orderBy(col("account_start_dt").desc)))
      .filter("ban_rank=1")
      .withColumn(
        "video_svc_status",
        when($"cust_acct_stat_desc" === "ACTIVE", "AC")
          .otherwise(when(
            $"cust_acct_stat_desc".isin("SUSPENDED", "PENDING DISCONNECT", "PENDING SUSPEND") && $"LAST_DISC_RSN_CD"
              .isin("SVC DISC - CUTOFF LEVEL 1", "SVC DISC - CUTOFF LEVEL 2"),
            "IS"
          ).otherwise(when($"cust_acct_stat_desc" === "PENDING", "VS")
            .otherwise(when(
              $"cust_acct_stat_desc"
                .isin("SUSPENDED", "PENDING DISCONNECT", "PENDING SUSPEND") && !$"LAST_DISC_RSN_CD"
                .isin("SVC DISC - CUTOFF LEVEL 1", "SVC DISC - CUTOFF LEVEL 2"),
              "VS"
            ).otherwise(when(
              $"cust_acct_stat_desc".isin("WRITEOFF", "DISCONNECTED", "VOIDED", "CUTOFF", "COLLECTIONS", "DELETED"),
              "CE")
              .otherwise("UK")))))
      )
      .withColumn("account_start_dt_utc", to_utc_timestamp($"account_start_dt", "US/Mountain"))

    //Creation of ev_dlr data frame with only selected columns
    ev_dlr_df = spark
      .table(s"$CONF_EV_MD_HIVE_DB.ev_dealer")
      .filter($"data_dt" === dlrMaxDt && $"business_line" === "DIRECTV")
      .select("dlr_acct_key", "dlr_name", "lead_dlr_name", "data_dt")

    ev_acc_dlr_df = ev_acc_df
      .join(ev_dlr_df, ev_acc_df.col("dlr_acct_key") === ev_dlr_df.col("dlr_acct_key"), "left")
      .drop(ev_dlr_df("data_dt"))
      .withColumn("attributes",
                  concat_ws(",",
                            $"video_svc_status",
                            $"zip",
                            $"account_start_dt_utc",
                            $"dlr_name",
                            $"lead_dlr_name",
                            $"dtv_dma_cd"))
  }

  def writeTablePartition(
      CONF_EV_CD_HIVE_DB: String,
      partitionDate: Int,
      finalData: DataFrame,
      noOfPartFiles: Int): Unit = {
    finalData.coalesce(noOfPartFiles).createOrReplaceTempView("finalData")
    spark.sql(s"""
                 Insert Overwrite Table $CONF_EV_CD_HIVE_DB.ev_comp_chnl_account
                 Partition(data_dt=$partitionDate)
                 Select * from finalData
  """.stripMargin)
  }

  def initialLoad(CONF_EV_CD_HIVE_DB: String): Unit = {
    import spark.implicits._
    //val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
    //val utcTimestamp = ZonedDateTime.now.withZoneSameInstant(ZoneId.of("UTC")).format(dateTimeFormat)
    val src_acc = ev_acc_dlr_df
      .withColumn("upd_ind", when($"status" === "Active", 1).otherwise(0))
      //.withColumn("load_ts",lit(utcTimestamp))
      .withColumn("load_ts", to_utc_timestamp(current_timestamp, Calendar.getInstance().getTimeZone().getID()))

    val finalData = src_acc
      .select("ban",
              "video_svc_status",
              "zip",
              "account_start_dt_utc",
              "dlr_name",
              "lead_dlr_name",
              "dtv_dma_cd",
              "upd_ind",
              "load_ts",
              "data_dt")
    writeTablePartition(CONF_EV_CD_HIVE_DB, accMaxDt, finalData, noOfPartFiles)
    // finalData.show
  }

  def deltaLoad(CONF_EV_CD_HIVE_DB: String, EV_DATA_LOAD_DT: Int): Unit = {

    val src_acc = ev_acc_dlr_df
      .as("src")
      .join(ev_cc_acc_df.as("tgt"), ev_acc_dlr_df.col("ban") === ev_cc_acc_df.col("DTV_BAN"), "fullouter")
      .select(
        coalesce(col("src.ban"), col("tgt.dtv_ban")).alias("ban"),
        when(col("src.ban").isNotNull, col("src.video_svc_status"))
          .otherwise(col("tgt.video_svc_status"))
          .alias("video_svc_status"),
        when(col("src.ban").isNotNull, col("src.zip")).otherwise(col("tgt.service_zip_cd")).alias("zip"),
        when(col("src.ban").isNotNull, col("src.account_start_dt_utc"))
          .otherwise(col("tgt.init_video_svc_estb_dt"))
          .alias("account_start_dt_utc"),
        when(col("src.ban").isNotNull, col("src.dlr_name")).otherwise(col("tgt.partner_name")).alias("dlr_name"),
        when(col("src.ban").isNotNull, col("src.lead_dlr_name"))
          .otherwise(col("tgt.dealer_name"))
          .alias("lead_dlr_name"),
        when(col("src.ban").isNotNull, col("src.dtv_dma_cd")).otherwise(col("tgt.dtv_dma_cd")).alias("dtv_dma_cd"),
        when(
          col("src.data_dt").isNotNull && coalesce(col("src.attributes"), lit("N")) =!= coalesce(col("tgt.attributes"),
                                                                                                 lit("N")),
          col("src.data_dt")).otherwise(col("tgt.src_data_dt")).alias("data_dt"),
        when(coalesce(col("src.attributes"), lit("N")) =!= coalesce(col("tgt.attributes"), lit("N")), lit(1))
          .otherwise(lit(0))
          .alias("upd_ind")
      )
      .withColumn("load_ts", to_utc_timestamp(current_timestamp, Calendar.getInstance().getTimeZone().getID()))
    val finalData = src_acc
      .select("ban",
              "video_svc_status",
              "zip",
              "account_start_dt_utc",
              "dlr_name",
              "lead_dlr_name",
              "dtv_dma_cd",
              "upd_ind",
              "load_ts",
              "data_dt")
    writeTablePartition(CONF_EV_CD_HIVE_DB, EV_DATA_LOAD_DT, finalData, noOfPartFiles)
    // finalData.show
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
    noOfPartFiles = config.getString("AccFileCnt").toInt

    def load(option: String) = option.toUpperCase() match {
      case "CREATE" => createTable(CONF_EV_CD_HIVE_DB, CONF_EV_CD_HIVE_TABLE_LOC)
      case "INITIAL" =>
        createTable(CONF_EV_CD_HIVE_DB, CONF_EV_CD_HIVE_TABLE_LOC)
        initDataFrames(CONF_EV_CD_HIVE_DB, CONF_EV_MD_HIVE_DB, partitionDate, partitionDate)
        initialLoad(CONF_EV_CD_HIVE_DB)
      case "DELTA" =>
        initDataFrames(CONF_EV_CD_HIVE_DB, CONF_EV_MD_HIVE_DB, prevpartitionDate, partitionDate)
        deltaLoad(CONF_EV_CD_HIVE_DB, partitionDate)
        partitionPurge(CONF_EV_CD_HIVE_DB,
                       CONF_EV_CD_HIVE_TABLE_LOC,
                       "EV_COMP_CHNL_ACCOUNT",
                       "data_dt",
                       RETENTION_DAYS.toInt)
      case _ => throw new IllegalArgumentException("Invalid Load Type Passed")
    }
    load(loadType)
  } //end of Main
}
