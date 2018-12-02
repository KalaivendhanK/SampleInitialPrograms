package com.att.eview.etl

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.io.File

import org.apache.log4j.Logger

object EvCompChnlAccountReceiver extends SparkSessionWrapper with PartitionCleanup {

  /*Initialize global variables */

  var custAcctAcrdXrefDF         = spark.emptyDataFrame
  var smrtCardDlyDF              = spark.emptyDataFrame
  var mergedDF                   = spark.emptyDataFrame
  var evCompChnlAccReceiverDF    = spark.emptyDataFrame
  var custAcctAcrdXrefMaxDT: Int = 0
  var smrtCrdMaxDT: Int          = 0
  var noOfPartFiles: Int         = 0

  /*Create Hive Table Method*/
  def createTable(CONF_EV_CD_HIVE_DB: String, CONF_EV_CD_HIVE_TABLE_LOC: String): Unit = {
    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS $CONF_EV_CD_HIVE_DB.EV_COMP_CHNL_ACCOUNT_RECEIVER(
          DTV_BAN STRING,
         |CAMC_SBSCRBR_ID STRING,
         |ACCESS_CARD_NUMBER STRING,
         |ACCESS_CARD_TYPE STRING,
         |IS_MIRRORED STRING,
         |STATUS_CD   INT COMMENT '1-Active,0-Non Active',
         |UPD_IND INT COMMENT '0-No Change,1-Updated',
         |LOAD_TS TIMESTAMP,
         |SRC_DATA_DT INT)
         |PARTITIONED BY (DATA_DT INT)
         |STORED AS ORC
         |LOCATION '$CONF_EV_CD_HIVE_TABLE_LOC/EV_COMP_CHNL_ACCOUNT_RECEIVER'
         |TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY") """.stripMargin
    )
  }

  /** Creation of data frame with only necessary data and columns  */
  def initDataFrames(
      CONF_EV_CD_HIVE_DB: String,
      CONF_EV_MD_HIVE_DB: String,
      CONF_EV_MD_DTVDW_HIVE_DB: String,
      EV_DATA_DT: Int,
      EV_DATA_LOAD_DT: Int): Unit = {

    import spark.implicits._

    //Fetching max partition value from source table
    custAcctAcrdXrefMaxDT = spark
      .sql(s"show partitions $CONF_EV_MD_DTVDW_HIVE_DB.cust_acct_acrd_xref")
      .collect()
      .map(A => A.toString().substring(9, 17))
      .filter(a => (a <= EV_DATA_LOAD_DT.toString()))
      .sortWith(_ > _)
      .head
      .toInt

    smrtCrdMaxDT = spark
      .sql(s"show partitions $CONF_EV_MD_DTVDW_HIVE_DB.smartcards_dly")
      .collect()
      .map(A => A.toString().substring(9, 17))
      .filter(a => (a <= EV_DATA_LOAD_DT.toString()))
      .sortWith(_ > _)
      .head
      .toInt

    //Creation of source data frame with only necessary data and columns
    custAcctAcrdXrefDF = spark
      .table(s"$CONF_EV_MD_DTVDW_HIVE_DB.cust_acct_acrd_xref")
      .select("cust_acct_num",
              "acrd_id",
              "camc_cust_id",
              "acrd_type_code",
              "acrd_mirror_flag",
              "acrd_stat_code",
              "last_upd_dt",
              "data_dt")
      .withColumn("srctbl", lit(0))
      .filter($"data_dt" === custAcctAcrdXrefMaxDT)
      .filter($"cust_acct_num".isNotNull)
      .filter($"acrd_id".isNotNull)
      .withColumn(s"rnum",
                  row_number().over(
                    Window
                      .partitionBy("cust_acct_num", "acrd_id")
                      .orderBy(col("last_upd_dt").desc)))
      .filter("rnum=1")
      .drop("last_upd_dt")

    smrtCardDlyDF = spark
      .table(s"$CONF_EV_MD_DTVDW_HIVE_DB.smartcards_dly")
      .select("acct_num",
              "smrtcrd_id",
              "camc_subscr_id",
              "smrtcrd_type",
              "sc_mirror_flg",
              "smrtcrd_stat",
              "lst_mod_dtim_dt",
              "data_dt")
      .withColumn("srctbl", lit(1))
      .filter($"data_dt" >= custAcctAcrdXrefMaxDT)
      .filter($"data_dt" <= EV_DATA_LOAD_DT.toString)
      .withColumn(s"rnum",
                  row_number().over(
                    Window
                      .partitionBy("acct_num", "smrtcrd_id")
                      .orderBy(col("lst_mod_dtim_dt").desc)))
      .filter("rnum=1")
      .filter($"acct_num".isNotNull)
      .filter($"smrtcrd_id".isNotNull)
      .drop("lst_mod_dtim_dt")

    /*Creation of previous day partition data frame */
    evCompChnlAccReceiverDF = spark
      .table(s"$CONF_EV_CD_HIVE_DB.ev_comp_chnl_account_receiver")
      .filter($"DATA_DT" === EV_DATA_DT)
      .withColumn("attributes",
                  concat_ws(",",
                            $"dtv_ban",
                            $"camc_sbscrbr_id",
                            $"access_card_number",
                            $"access_card_type",
                            $"is_mirrored",
                            $"status_cd"))

    /*Creation of merged data frame as per use case */
    mergedDF = smrtCardDlyDF
      .union(custAcctAcrdXrefDF)
      .withColumn(s"rnum",
                  row_number().over(Window
                    .partitionBy("acct_num", "smrtcrd_id")
                    .orderBy(col("srctbl").desc)))
      .filter("rnum=1")
      .filter($"smrtcrd_stat" =!= "PEND")
      .withColumn(
        "status_cd",
        when(
          $"smrtcrd_stat" === "DISC" || $"smrtcrd_stat" === "DCRD" || $"smrtcrd_stat" === "EXPD" || $"smrtcrd_stat" === "SWPD",
          0).otherwise(1))
      .withColumn("upd_ind", when($"status_cd" === 1, 1).otherwise(0))
      .drop("rnum")
      .drop("srctbl")
      .withColumn("load_ts", to_utc_timestamp(current_timestamp, Calendar.getInstance().getTimeZone().getID()))
      .withColumn(
        "attributes",
        concat_ws(",", $"acct_num", $"camc_subscr_id", $"smrtcrd_id", $"smrtcrd_type", $"sc_mirror_flg", $"status_cd"))

  }

  def initialLoad(CONF_EV_CD_HIVE_DB: String): Unit = {

    val finalData = mergedDF.select(
      col("acct_num").alias("dtv_ban"),
      col("camc_subscr_id").alias("camc_sbscrbr_id"),
      col("smrtcrd_id").alias("access_card_number"),
      col("smrtcrd_type").alias("access_card_type"),
      col("sc_mirror_flg").alias("is_mirrored"),
      col("status_cd").alias("status_cd"),
      col("upd_ind").alias("upd_ind"),
      col("load_ts").alias("load_ts"),
      col("data_dt").alias("src_data_dt")
    )
    writeTablePartition(CONF_EV_CD_HIVE_DB, smrtCrdMaxDT, finalData, noOfPartFiles)
  }

  def deltaLoad(CONF_EV_CD_HIVE_DB: String, EV_DATA_LOAD_DT: Int): Unit = {
    val deltaDF = mergedDF
      .as("src")
      .join(
        evCompChnlAccReceiverDF.as("tgt"),
        mergedDF.col("acct_num") === evCompChnlAccReceiverDF.col("dtv_ban") && mergedDF
          .col("smrtcrd_id") === evCompChnlAccReceiverDF.col("access_card_number"),
        "fullouter"
      )
      .select(
        coalesce(col("src.acct_num"), col("tgt.dtv_ban")).alias("dtv_ban"),
        when(col("src.smrtcrd_id").isNotNull, col("src.camc_subscr_id"))
          .otherwise(col("tgt.camc_sbscrbr_id"))
          .alias("camc_sbscrbr_id"),
        when(col("src.smrtcrd_id").isNotNull, col("src.smrtcrd_id"))
          .otherwise(col("tgt.access_card_number"))
          .alias("access_card_number"),
        when(col("src.smrtcrd_id").isNotNull, col("src.smrtcrd_type"))
          .otherwise(col("tgt.access_card_type"))
          .alias("access_card_type"),
        when(col("src.smrtcrd_id").isNotNull, col("src.sc_mirror_flg"))
          .otherwise(col("tgt.is_mirrored"))
          .alias("is_mirrored"),
        when(col("src.smrtcrd_id").isNotNull, col("src.status_cd")).otherwise(col("tgt.status_cd")).alias("status_cd"),
        when(
          col("src.data_dt").isNotNull && coalesce(col("src.attributes"), lit("N")) =!= coalesce(col("tgt.attributes"),
                                                                                                 lit("N")),
          col("src.data_dt")).otherwise(col("tgt.src_data_dt")).alias("src_data_dt"),
        when(coalesce(col("src.attributes"), lit("N")) =!= coalesce(col("tgt.attributes"), lit("N")), lit(1))
          .otherwise(lit(0))
          .alias("upd_ind")
      )
      .withColumn("load_ts", to_utc_timestamp(current_timestamp, Calendar.getInstance().getTimeZone().getID()))

    val finalData = deltaDF
      .select("dtv_ban",
              "camc_sbscrbr_id",
              "access_card_number",
              "access_card_type",
              "is_mirrored",
              "status_cd",
              "upd_ind",
              "load_ts",
              "src_data_dt")
    writeTablePartition(CONF_EV_CD_HIVE_DB, EV_DATA_LOAD_DT, finalData, noOfPartFiles)

  }

  def writeTablePartition(
      CONF_EV_CD_HIVE_DB: String,
      partitionDate: Int,
      finalData: DataFrame,
      noOfPartFiles: Int): Unit = {
    finalData.coalesce(noOfPartFiles).createOrReplaceTempView("finalData")
    spark.sql(s"""
                 Insert Overwrite Table $CONF_EV_CD_HIVE_DB.ev_comp_chnl_account_receiver
                 Partition(data_dt=$partitionDate)
                 Select * from finalData
  """.stripMargin)
  }
  def main(args: Array[String]): Unit = {
    val exec_env          = args(0) // Load type Initial or Incremental Load
    val loadType          = args(1) // Load type Initial or Incremental Load
    val prevpartitionDate = args(2).toInt
    val partitionDate     = args(3).toInt
    val log               = Logger.getLogger(getClass.getName)
    /* Default System Properties */
    val config = ConfigFactory
      .parseFile(new File("application.conf"))
      .withFallback(ConfigFactory.load())
      .resolve
      .getConfig(exec_env)

    val CONF_EV_CD_HIVE_DB        = config.getString("CONF_EV_CD_HIVE_DB")
    val CONF_EV_MD_HIVE_DB        = config.getString("CONF_EV_MD_HIVE_DB")
    val CONF_EV_MD_HIVE_TABLE_LOC = config.getString("CONF_EV_MD_HIVE_TABLE_LOC")
    val CONF_EV_CD_HIVE_TABLE_LOC = config.getString("CONF_EV_CD_HIVE_TABLE_LOC")
    val CONF_EV_MD_DTVDW_HIVE_DB  = config.getString("CONF_EV_MD_DTVDW_HIVE_DB")
    val RETENTION_DAYS            = config.getString("RETENTION_DAYS")

    noOfPartFiles = config.getString("AccRcvrFileCnt").toInt

    log.info(s"exec_env:$exec_env")
    log.info(s"CONF_EV_CD_HIVE_DB:$CONF_EV_CD_HIVE_DB")
    log.info(s"CONF_EV_MD_HIVE_DB:$CONF_EV_MD_HIVE_DB")
    log.info(s"CONF_EV_MD_HIVE_TABLE_LOC:$CONF_EV_MD_HIVE_TABLE_LOC")
    log.info(s"CONF_EV_CD_HIVE_TABLE_LOC:$CONF_EV_CD_HIVE_TABLE_LOC")
    log.info(s"CONF_EV_MD_DTVDW_HIVE_DB:$CONF_EV_MD_DTVDW_HIVE_DB")
    log.info(s"RETENTION_DAYS=$RETENTION_DAYS")

    def load(option: String) = option.toUpperCase() match {
      case "CREATE" =>
        createTable(CONF_EV_CD_HIVE_DB, CONF_EV_CD_HIVE_TABLE_LOC)

      case "INITIAL" =>
        createTable(CONF_EV_CD_HIVE_DB, CONF_EV_CD_HIVE_TABLE_LOC)
        initDataFrames(CONF_EV_CD_HIVE_DB, CONF_EV_MD_HIVE_DB, CONF_EV_MD_DTVDW_HIVE_DB, partitionDate, partitionDate)
        initialLoad(CONF_EV_CD_HIVE_DB)

      case "DELTA" =>
        initDataFrames(CONF_EV_CD_HIVE_DB,
                       CONF_EV_MD_HIVE_DB,
                       CONF_EV_MD_DTVDW_HIVE_DB,
                       prevpartitionDate,
                       partitionDate)
        deltaLoad(CONF_EV_CD_HIVE_DB, partitionDate)

        partitionPurge(CONF_EV_CD_HIVE_DB,
                       CONF_EV_CD_HIVE_TABLE_LOC,
                       "EV_COMP_CHNL_ACCOUNT_RECEIVER",
                       "data_dt",
                       RETENTION_DAYS.toInt)

      case _ => throw new IllegalArgumentException("Invalid Load Type Passed")
    }
    load(loadType)
  }

}
