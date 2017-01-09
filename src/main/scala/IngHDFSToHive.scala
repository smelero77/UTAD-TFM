package org.dummy


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.mutable


import org.apache.spark.sql.hive.HiveContext


object IngHDFSToHive extends App{


  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
    .setAppName("IngHDFSToES")
    .setMaster("local[*]")
    .setSparkHome("$SPARK_HOME")
    .set("es.nodes", "localhost")

  //Contexto de SPARK

  val sparkContext = new SparkContext(sparkConfig)
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

  //Contexto de HIVE

  val hiveContext = new HiveContext(sparkContext)
  hiveContext.setConf("hive.metastore.uris", "thrift://localhost:9083")


  // Leer vuelos

  val df_Flights = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("hdfs://localhost:9000/user/Flights/Flights.csv")

  df_Flights.registerTempTable("Flights")


  // Leer tipos de avion

  val df_Planedata = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("hdfs://localhost:9000/user/Planedata/planedata.csv")

  df_Planedata.registerTempTable("Planedata")

  // Leer aerolineas

  val df_Carriers = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("hdfs://localhost:9000/user/Carriers/carriers.csv")

  df_Carriers.registerTempTable("Carriers")


  // Leer aeropuertos

  val df_Airports = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("hdfs://localhost:9000/user/Airports/airports.csv")



  df_Airports.registerTempTable("Airports")



  val df_completo = sqlContext.sql(
    "SELECT " +
      "Flights.Year," +
      "Flights.Month," +
      "Flights.DayofMonth," +
      "Flights.DepTime," +
      "Flights.UniqueCarrier," +
      "Flights.FlightNum," +
      "Flights.TailNum," +
      "Flights.ArrDelay," +
      "Flights.Origin," +
      "Flights.Dest," +
      "CONCAT(CONCAT(Flights.Origin, '✈'),Flights.Dest) AS ruta ," +
      "Planedata.manufacturer," +
      "Planedata.model," +
      "Planedata.aircraft_type,"  +
      "Planedata.engine_type"  +
      " FROM Flights, " +
      "      Planedata "  +
      "WHERE TRIM(Flights.TailNum) = TRIM(Planedata.tailnum)" +
      "LIMIT 100000"
  ).registerTempTable("Flights_Planedata")

  sqlContext.sql(
    "SELECT " +
      "Flights_Planedata.*," +
      "Carriers.Description"  +
      " FROM Flights_Planedata, " +
      "      Carriers " +
      "WHERE TRIM(Flights_Planedata.UniqueCarrier) = TRIM(Carriers.Code) "
  ).registerTempTable("Flights_Planedata_Carriers")


  sqlContext.sql(
    "SELECT " +
      "Flights_Planedata_Carriers.*," +
      "Airports.airport AS airporto,"  +
      "Airports.city AS cityo,"  +
      "Airports.state AS stateo,"  +
      "Airports.lat AS lato,"  +
      "Airports.long AS longo "  +
      " FROM Flights_Planedata_Carriers, " +
      "      Airports " +
      "WHERE TRIM(Flights_Planedata_Carriers.Origin) = TRIM(Airports.iata) "
  ).registerTempTable("Flights_Planedata_Carriers_AirportO")

   sqlContext.sql(
    "SELECT " +
      "Flights_Planedata_Carriers_AirportO.*," +
      "Airports.airport AS airportd,"  +
      "Airports.city AS cityd,"  +
      "Airports.state AS stated,"  +
      "Airports.lat AS latd,"  +
      "Airports.long AS longd "  +
      " FROM Flights_Planedata_Carriers_AirportO, " +
      "      Airports " +
      "WHERE TRIM(Flights_Planedata_Carriers_AirportO.Dest) = TRIM(Airports.iata)  "
  ).registerTempTable("Flights_Total")

  val df_full =  sqlContext.sql(
      "SELECT " +
        "Flights_Total.Year," +
        "Flights_Total.Month," +
        "Flights_Total.DayofMonth," +
        "Flights_Total.DepTime," +
        "Flights_Total.UniqueCarrier," +
        "Flights_Total.FlightNum," +
        "Flights_Total.TailNum," +
        "Flights_Total.ArrDelay," +
        "Flights_Total.Origin," +
        "Flights_Total.Dest," +
        "Flights_Total.Description," +
        "Flights_Total.airporto  AS airport,"  +
        "Flights_Total.cityo  AS city,"  +
        "Flights_Total.stateo as state ,"  +
        "Flights_Total.lato AS lat,"  +
        "Flights_Total.longo AS long, "  +
        "Flights_Total.manufacturer," +
        "Flights_Total.model," +
        "Flights_Total.aircraft_type,"  +
        "Flights_Total.engine_type, "  +
        "CONCAT(CONCAT(Flights_Total.Origin, '✈'),Flights_Total.Dest) AS ruta ," +
        "1 AS pathorigin, " +
        "0 AS numrecords " +
        "FROM Flights_Total " +
        "UNION " +
        "SELECT " +
        "Flights_Total.Year," +
        "Flights_Total.Month," +
        "Flights_Total.DayofMonth," +
        "Flights_Total.DepTime," +
        "Flights_Total.UniqueCarrier," +
        "Flights_Total.FlightNum," +
        "Flights_Total.TailNum," +
        "Flights_Total.ArrDelay," +
        "Flights_Total.Origin," +
        "Flights_Total.Dest," +
        "Flights_Total.Description," +
        "Flights_Total.airportd  as airport,"  +
        "Flights_Total.cityd  as city,"  +
        "Flights_Total.stated as  state  ,"  +
        "Flights_Total.latd AS lat,"  +
        "Flights_Total.longd AS long ,"  +
        "Flights_Total.manufacturer," +
        "Flights_Total.model," +
        "Flights_Total.aircraft_type,"  +
        "Flights_Total.engine_type,"  +
        "CONCAT(CONCAT(Flights_Total.Origin, '✈'),Flights_Total.Dest) AS ruta ," +
        "2 AS pathdest ," +
        "1 AS numrecords " +
        "FROM Flights_Total")
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("hdfs://localhost:9000/user/All/DelayFlights")

  hiveContext.sql("USE default")

  val df_Hive_Delays = hiveContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("hdfs://localhost:9000/user/All/DelayFlights")

  //df_Hive_Delays.saveAsTable("Delays")

  sys.exit(0)


}

