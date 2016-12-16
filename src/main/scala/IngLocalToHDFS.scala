package org.dummy

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable



object IngLocalToHDFS extends App{


  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf     // nueva configuracion
  sparkConfig.setMaster("local[*]")   // set master que en este caso indica que es cluster local
  sparkConfig.setAppName("IngLocalToHDFS")      // nombre de la aplicacion
  val sparkContext = new SparkContext(sparkConfig)  //contexto de spark

  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext) // contexto de Spark SQL


  // Ingestión de ficheros desde local a HDFS

  val airports = sparkContext.textFile("/home/sergio/TFM_Ficheros/Airports/airports.csv")

  airports.saveAsTextFile("hdfs://localhost:9000/user/Airports/airports.csv")

  val carriers = sparkContext.textFile("/home/sergio/TFM_Ficheros/Carriers/carriers.csv")

  carriers.saveAsTextFile("hdfs://localhost:9000/user/Carriers/carriers.csv")

  val planedata = sparkContext.textFile("/home/sergio/TFM_Ficheros/PlaneData/plane-data.csv")

  planedata.saveAsTextFile("hdfs://localhost:9000/user/Planedata/planedata.csv")


  // Carga de todos los ficheros con los datos de los vuelos a un único fichero en HDFS

  val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
  .load("hdfs://localhost:9000/user/Planedata/planedata.csv")

  df.show(1000);

  val r1 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2007.csv")
  val r2 = sparkContext.textFile("/home/sergio/TFM_Ficheros/Flights/2008.csv")
  val rdds = Seq(r1, r2)
  val bigRdd = sparkContext.union(rdds)
  bigRdd.saveAsTextFile("hdfs://localhost:9000/user/Flights/Flights.csv")


   /*
  //Athlete,Age,Country,Game,Date,Sport,Gold,Silver,Bronze,Total

  //Ejercicio 1. Diferencia de medallas entre USA y CHINA

  println("***** Ejercicio 1. Diferencia de medallas entre USA y CHINA ***** ")

  sqlContext.sql("SELECT Game,SUM(Medallas.Total),TablaChina.CHINATOTAL," +
    "CASE  WHEN  SUM(Medallas.Total) - TablaChina.CHINATOTAL < 0 THEN (SUM(Medallas.Total) - TablaChina.CHINATOTAL) * -1 ELSE SUM(Medallas.Total) - TablaChina.CHINATOTAL END " +
    "FROM Medallas FULL OUTER JOIN (SELECT Game AS ANNO, SUM(Total) AS CHINATOTAL FROM Medallas WHERE  Country = 'China' GROUP BY Game) TablaChina "+
    "ON Medallas.Game = TablaChina.ANNO "+
    "WHERE  Medallas.Country = 'United States' " +
    "GROUP BY Medallas.Game,TablaChina.CHINATOTAL " +
    "ORDER BY Medallas.Game").foreach(println)


  val filas = sparkContext.textFile("OlympicAthletesNoHeader.csv")


  //Ejercicio 2. Número máximo de medallas por país

  println("***** Ejercicio 2. Número máximo de medallas por país ***** ")


  val map1 = filas.map(s => s.split(",")).map(s => (s(2) , s(3)) -> ((s(6).toInt * 3 ) + (s(7).toInt * 2) + (s(8).toInt)))
    .reduceByKey((x,y) => x + y)

  val map2 = map1.map(s => ((s._1._1) ,List( s._2 -> s._1._2 ))).sortBy( s=> s._2.head._1)
    .reduceByKey(_++_).mapValues(linea => linea.last).collect().foreach(l=>println(l))


  // Ejercicio 3. Ejercicio 3. Los mejores tres medallistas por olimpiada

  println("***** Ejercicio 3. Los mejores tres medallistas por olimpiada ***** ")

  val ejercicio3 = filas.map(s => s.split(",")).map(s => ((s(3)), List(s(0) -> s(9)))).sortBy( s=> s._1)
      .reduceByKey(_++_).mapValues(linea => linea.take(3)).collect()
      .foreach(l => println(l))

  */



sys.exit(0)


}

