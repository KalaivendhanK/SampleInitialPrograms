package com.att.eview.etl
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat
import org.apache.log4j.Logger

trait PartitionCleanup  extends SparkSessionWrapper{
  val fs= FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val log = Logger.getLogger(getClass.getName)

  import spark.implicits._
  def partitionRemove(CONF_EV_CD_HIVE_DB: String, CONF_EV_CD_HIVE_TABLE_LOC: String,TABLE_NAME: String,PARTITION_VALUE:String,PARTITION_NAME:String ): Unit ={

    var status:Boolean=false
    if(CONF_EV_CD_HIVE_TABLE_LOC.isEmpty || CONF_EV_CD_HIVE_DB.isEmpty || TABLE_NAME.isEmpty || PARTITION_VALUE.isEmpty || PARTITION_NAME.isEmpty )
    {

      log.info("All parameters are not available to perform partition cleanup")
      log.info(s"CONF_EV_CD_HIVE_TABLE_LOC=$CONF_EV_CD_HIVE_TABLE_LOC")
      log.info(s"CONF_EV_CD_HIVE_DB=$CONF_EV_CD_HIVE_DB")
      log.info(s"TABLE_NAME=$TABLE_NAME")
      log.info(s"PARTITION_VALUE=$PARTITION_VALUE")
      log.info(s"PARTITION_NAME=$PARTITION_NAME")
    }
    else
    {
      spark.sql(s"alter table  $CONF_EV_CD_HIVE_DB.$TABLE_NAME drop partition($PARTITION_NAME=$PARTITION_VALUE)")
      status=fs.delete(new Path(s"$CONF_EV_CD_HIVE_TABLE_LOC$TABLE_NAME/$PARTITION_NAME=$PARTITION_VALUE"),true)
      if(status)
        log.info(s"Successfully removed $CONF_EV_CD_HIVE_TABLE_LOC$TABLE_NAME/$PARTITION_NAME=$PARTITION_VALUE")
      else
        log.info(s"Error in removing $CONF_EV_CD_HIVE_TABLE_LOC$TABLE_NAME/$PARTITION_NAME=$PARTITION_VALUE")

    }



  }

  def partitionPurge(CONF_EV_CD_HIVE_DB: String, CONF_EV_CD_HIVE_TABLE_LOC: String,TABLE_NAME: String, PARTITION_NAME: String,RETENTION_DAYS: Int ): Unit = {

    val timeZone = TimeZone.getTimeZone("UTC")
    val now = Calendar.getInstance(timeZone)
    val simpleFormat = new SimpleDateFormat("yyyyMMdd")
    now.add(Calendar.DATE,-RETENTION_DAYS)
    val oldDate=simpleFormat.format(now.getTime)
    log.info(s"Date After Subtracting Retention Days=$oldDate")

    val oldPartitions = spark.sql(s"show partitions $CONF_EV_CD_HIVE_DB.$TABLE_NAME ").select($"partition")
      .map(A => A.toString().substring(9, 17).toInt)
      .filter(a => a < oldDate.toInt)
      .toDF()

    oldPartitions.select($"value").collect().map(A=>A.toString().substring(1,9)).map(a=>a.toInt).map(a=>partitionRemove(CONF_EV_CD_HIVE_DB,CONF_EV_CD_HIVE_TABLE_LOC,TABLE_NAME,a.toString,PARTITION_NAME))


  }

}
