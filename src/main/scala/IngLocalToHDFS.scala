package org.dummy

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable



object IngLocalToHDFS extends App{


  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  //Contexto de SPARK
  val sparkConfig = new SparkConf
  sparkConfig.setMaster("local[*]")
  sparkConfig.setAppName("IngLocalToHDFS")
  val sparkContext = new SparkContext(sparkConfig)
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)


  // Ingestión de ficheros desde local a HDFS

  val airports = sparkContext.textFile("/home/sergio/TFM_Ficheros/Airports/airports.csv")

  airports.saveAsTextFile("hdfs://localhost:9000/user/Airports/airports.csv")

  val carriers = sparkContext.textFile("/home/sergio/TFM_Ficheros/Carriers/carriers.csv")

  carriers.saveAsTextFile("hdfs://localhost:9000/user/Carriers/carriers.csv")

  val planedata = sparkContext.textFile("/home/sergio/TFM_Ficheros/PlaneData/plane-data.csv")

  planedata.saveAsTextFile("hdfs://localhost:9000/user/Planedata/planedata.csv")

  val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
  .load("hdfs://localhost:9000/user/Planedata/planedata.csv")

  // Carga de todos los ficheros con los datos de los vuelos a un único fichero en HDFS

  val r1 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1987.csv")
  val r2 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1988.csv")
  val r3 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1989.csv")
  val r4 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1990.csv")
  val r5 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1991.csv")
  val r6 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1992.csv")
  val r7 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1993.csv")
  val r8 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1994.csv")
  val r9 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1995.csv")
  val r10 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1996.csv")
  val r11 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1997.csv")
  val r12 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1998.csv")
  val r13 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/1999.csv")
  val r14= sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2000.csv")
  val r15 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2001.csv")
  val r16 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2002.csv")
  val r17 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2003.csv")
  val r18 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2004.csv")
  val r19 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2005.csv")
  val r20 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2006.csv")
  val r21 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2007.csv")
  val r22= sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2008.csv")
  val rdds = Seq(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22)
  val bigRdd = sparkContext.union(rdds)
  bigRdd.saveAsTextFile("hdfs://localhost:9000/user/Flights/Flights.csv")


sys.exit(0)


}

