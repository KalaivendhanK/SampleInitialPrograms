package main.scala

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.reflect.api.materializeTypeTag

case class mnClassabs(Day: Int, JD: Int, Month: Int, Year: Int, TAVE: Double, TMAX: Double, TMIN: Double);
case class mnClass(Day: Int, JD: Int, Month: Int, Stateid: String, Year: Int, Snow: Double, Precp: Double, TAVE: Double, TMAX: Double, TMIN: Double);

object workingWithSpark {
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").config("hive.metastore.uri","thrift://127.0.0.1:9028").appName("DataAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //Creating the schema with StructType Class for all the columns in mn file
    //It is used to provide the schema for DataFrame
    val mnSchema = StructType(Array(
      StructField("Day", IntegerType),
      StructField("JD", IntegerType),
      StructField("Month", IntegerType),
      StructField("Stateid", StringType),
      StructField("Year", IntegerType),
      StructField("Snow", DoubleType),
      StructField("Precp", DoubleType),
      StructField("TAVE", DoubleType),
      StructField("TMAX", DoubleType),
      StructField("TMIN", DoubleType)))
    //Creating the schema with StructType Class for required the columns in mn file
    //It is used while creating DF through RDD of rows
    val mnSchemaAbs = StructType(Array(
      StructField("Day", IntegerType),
      StructField("JD", IntegerType),
      StructField("Month", IntegerType),
      StructField("Year", IntegerType),
      StructField("TAVE", DoubleType),
      StructField("TMAX", DoubleType),
      StructField("TMIN", DoubleType)))

    def rddThings = {
      //Reading the file and creating it as RDD
      val mnFileRDD = spark.sparkContext.
        textFile("file:///D:/Kalai/spark/BigData Sample files/BigDataAnalyticswithSpark-master/BigDataAnalyticswithSpark-master/MN212142_9392.csv")
      //Working with legacy rdd and its functions
      //Splitting RDD into separate cols and putting it into the case class
      val mnCaseClassRDD = mnFileRDD.filter(!_.contains("Day")).filter(!_.contains(".")).map { line =>
        val a = line.split(",")(0).toInt; val b = line.split(",")(1).toInt; val c = line.split(",")(2).toInt; val d = line.split(",")(4).toInt;
        val e = line.split(",")(7).toDouble; val f = line.split(",")(8).toDouble; val g = line.split(",")(9).toDouble
        mnClassabs(a, b, c, d, e, f, g)
      }
      //println(s"Printing out the rdd of case class type")
      //mnCaseClassRDD.collect foreach println
      //Splitting the RDD into required fields and converting them to Row of values
      val mnFileSplits = mnFileRDD.filter(!_.contains("Day")).filter(!_.contains(".")) map { line =>
        val a = line.split(",")(0).toInt; val b = line.split(",")(1).toInt; val c = line.split(",")(2).toInt; val d = line.split(",")(4).toInt;
        val e = line.split(",")(7).toDouble; val f = line.split(",")(8).toDouble; val g = line.split(",")(9).toDouble
        Row(a, b, c, d, e, f, g)
      }
      //Creating the DataFrame from the RDD of rows and the schema defined above with required fields
      val DFfromRDD = spark.createDataFrame(mnFileSplits, mnSchemaAbs) //Need to check - Failing due to numberFormatException
      val computeDFfromRDD = DFfromRDD.groupBy('Year, 'Month).agg(count('Day))
      println(s"Printing out the average derived from the RDD of rows and schema using the StructType Class")
      computeDFfromRDD show
      //Creating the DataFrame from the RDD of rows and schema using the case class #mnClassabs
      val DFfromRDDandCaseClass = spark.createDataFrame(mnFileSplits, Encoders.product[mnClassabs].schema) //Need to check - Failing due to numberFormatException
      // println(s"Printing out the data derived from the RDD of rows and schema using the case class #mnClassabs")
      //DFfromRDDandCaseClass show

    }

    def dataFrameThings = {
      //Reading the file and creating the Data Frame and applying the schema defined above with all the fields
      val fileDF = spark.read.schema(mnSchema).option("header", true).
        csv("file:///D:/Kalai/spark/BigData Sample files/BigDataAnalyticswithSpark-master/BigDataAnalyticswithSpark-master/MN212142_9392.csv")
      //Calculating the average of tave for each year through sparkSQL
      val DFDayAverage = fileDF.groupBy("Year").agg(avg('TAVE)).withColumnRenamed("avg(TAVE)", "AverageTempByYear").orderBy('Year)
      println(s"Printing the average calculated through DataFrame and sql like functions")
      DFDayAverage show //Printing it out

      //Creating the temporary table from the Data Frame
      fileDF.createOrReplaceTempView("mnTable")
      //Calculating the average of tave from the temp table created
      val DFDayAverageSQL = spark.sql(""" Select year,avg(TAVE) as AverageTempByYearFromTable from mnTable 
                              group by year order by year asc """)
      println(s"Printing the average calculated through the registered temporary table and pure sql functions")
      DFDayAverageSQL show //Printing it out

    }

    def dataSetThings = {
      //Creating the dataset through DataFrame by applying  'as' function and passing case class as a type
      //and transforming the untyped dataset to the dataset of case class type
      val mnFileDSfromDF = spark.read.option("header", true).schema(Encoders.product[mnClass].schema) //creating schema from the case class
        .csv("file:///D:/Kalai/spark/BigData Sample files/BigDataAnalyticswithSpark-master/BigDataAnalyticswithSpark-master/MN212142_9392.csv")
        .as[mnClass]
      println(s"Printing the data from the dataset derived from dataframes")
      mnFileDSfromDF show
      //Creating the dataset directly while reading the file and converting it to typed dataset by spliting and storing them in case class
      val mnFileDS = spark.read.textFile("file:///D:/Kalai/spark/BigData Sample files/BigDataAnalyticswithSpark-master/BigDataAnalyticswithSpark-master/MN212142_9392.csv")
      val mnFileTypedDS = mnFileDS.filter(!_.contains("Day")).filter(!_.contains(".")).map { line =>
        val a = line.split(",")(0).toInt; val b = line.split(",")(1).toInt; val c = line.split(",")(2).toInt; val d = line.split(",")(4).toInt;
        val e = line.split(",")(7).toDouble; val f = line.split(",")(8).toDouble; val g = line.split(",")(9).toDouble
        mnClassabs(a, b, c, d, e, f, g)
      }
      mnFileTypedDS.createOrReplaceTempView("DatasetTable")
      val mnSQLDS = spark.sql("""select * from DatasetTable""")
      println(s"Printing the data from the table created through dataset")
      mnSQLDS.show
      println(s"Printing the data from the dataset created directly while reading the file")
      mnFileTypedDS show
    }
    //Calling the functions
    //rddThings
    //dataFrameThings
    //dataSetThings
    spark.sql("""show databases""").show
    spark.stop()
  }
}