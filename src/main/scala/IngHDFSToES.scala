package org.dummy

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.mutable



object IngHDFSToES extends App{


  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
    .setAppName("IngHDFSToES")
    .setMaster("local[*]")
    .setSparkHome("$SPARK_HOME")
    .set("es.nodes", "localhost")
    .set("es.port", "9200")
    .set("es.index.auto.create", "true")
    .set("es.field.read.empty.as.null", "false")
    .set("es.net.http.auth.user", "sergio")
    .set("es.net.http.auth.pass", "sergio77")


  val sparkContext = new SparkContext(sparkConfig)  //contexto de spark

  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext) // contexto de Spark SQL

  // Carga de todos los ficheros con los datos de los vuelos a un único fichero en HDFS

 /*val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("hdfs://localhost:9000/user/Airports/airports.csv")*/

 /* val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://localhost:9000/user/Airports/airports.csv")*/

  val rddAirports = sparkContext.textFile("hdfs://localhost:9000/user/Airports/airports.csv")


  val header = rddAirports.first()
  val rddAirports2 = rddAirports.filter(row => row != header)

  val rd = rddAirports2.filter(_.nonEmpty)
    .map(_.replaceAll(",NA,NA,", ",\"A\",\"A\","))
    .map(_.replaceAll(", ", " "))
    .map(_.replaceAll("w,I", "w I"))
    .map(_.replaceAll("Reading Muni,Gen", "Reading Muni Gen"))
    .map(_.replaceAll("Lawrence County Airpark,Inc", "Lawrence County Airpark Inc"))
    .map( linea => {
       val iata = "{" + "\"iata\":"+ linea.split(",")(0) + ","
       val airport =  "\"airport\":" +  linea.split(",")(1) + ","
       val city = "\"city\":" +  linea.split(",")(2) + ","
       val state = "\"state\":" +   linea.split(",")(3) + ","
       val country = "\"country\":"  + linea.split(",")(4) + ","
       val coordenada = "\"coordenada\":\"" + linea.split(",")(5) + "," + linea.split(",")(6)  + "\"}"
       val salida = iata + airport + city + state + country + coordenada
       salida
  })



  //
rd.foreach(linea => println(linea))
      //df.printSchema()


  //df.show(20);

 //df.toJSON.foreach(l => println(l))

 EsSpark.saveJsonToEs(rd,"airports/query1")

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

