package org.dummy

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.mutable

object Analytics extends App{


  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
    .setAppName("Analytics")
    .setMaster("local[*]")
    .setSparkHome("$SPARK_HOME")
    .set("es.nodes", "localhost")
    .set("es.port", "9200")
    .set("es.index.auto.create", "true")
    .set("es.field.read.empty.as.null", "false")
    .set("es.net.http.auth.user", "sergio")
    .set("es.net.http.auth.pass", "password")


  // Contexto de Spark
  val sparkContext = new SparkContext(sparkConfig)

  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)


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

  // Número total de vuelos  para cada mes

  val df_completo = sqlContext.sql(
    "SELECT " +
    "Flights.Year," +
    "Flights.Month," +
    "Flights.DayofMonth," +
    "Flights.DayOfWeek,"+
    "Flights.DepTime," +
    "Flights.CRSDepTime," +
    "Flights.ArrTime," +
    "Flights.CRSArrTime," +
    "Flights.UniqueCarrier," +
    "Flights.FlightNum," +
    "Flights.TailNum," +
    "Flights.ActualElapsedTime," +
    "Flights.CRSElapsedTime," +
    "Flights.AirTime," +
    "Flights.ArrDelay," +
    "Flights.DepDelay," +
    "Flights.Origin," +
    "Flights.Dest," +
    "Flights.Distance," +
    "Flights.TaxiIn," +
    "Flights.TaxiOut," +
    "Flights.Cancelled," +
    "Flights.CancellationCode," +
    "Flights.Diverted," +
    "Flights.CarrierDelay," +
    "Flights.WeatherDelay," +
    "Flights.NASDelay," +
    "Flights.SecurityDelay," +
    "Flights.LateAircraftDelay," +
    "Planedata.manufacturer," +
    "Planedata.model," +
    "Planedata.aircraft_type,"  +
    "Planedata.engine_type,"  +
    "Planedata.year " +
      " FROM Flights, " +
      "      Planedata "  +
      "WHERE TRIM(Flights.TailNum) = TRIM(Planedata.tailnum) "
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

  val df_full = sqlContext.sql(
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
  ).registerTempTable("Flights_Full")



  // Cálculo de las métricas

  // ****** 1º Calculamos vuelos y retrasos totales por año  ******************

  sqlContext.sql(
    "SELECT Year, COUNT(*) AS FlightTotYear" +
      " FROM Flights_Full " +
       "GROUP BY Year ").registerTempTable("Flights_Year")

   sqlContext.sql(
    "SELECT Year, COUNT(*) AS DelayTotYear" +
      " FROM Flights_Full " +
      " WHERE ArrDelay >= 15 " +
      "GROUP BY Year ").registerTempTable("Delay_Year")

   val rdd_delay_year = sqlContext.sql(
     "SELECT Flights_Year.Year, DelayTotYear, FlightTotYear,  (DelayTotYear / FlightTotYear)  AS YearRatio" +
      " FROM Flights_Year, " +
      "      Delay_Year" +
      " WHERE Flights_Year.Year = Delay_Year.Year").map(row => {
     val FlightsYear = "{" + "\"FlightsYear\":\"" + row.getAs[String](0)  + "\","
     val DelayTotYear = "\"DelayTotYear\":" + row(1) + ","
     val FlightTotYear = "\"FlightTotYear\":" + row(2)  + ","
     val YearRatio = "\"YearRatio\":" + row(3)  + "}"
     FlightsYear + DelayTotYear + FlightTotYear + YearRatio
   })

  EsSpark.saveJsonToEs(rdd_delay_year,"delay/year")

  println ("YearDelay")


  // ****** 2º Calculamos vuelos y retrasos totales mes  ******************
  sqlContext.sql(
    "SELECT Month, COUNT(*) AS FlightTotMonth" +
      " FROM Flights_Full " +
      "GROUP BY Month ").registerTempTable("Flights_Month")

  sqlContext.sql(
    "SELECT Month, COUNT(*) AS DelayTotMonth" +
      " FROM Flights_Full " +
      " WHERE ArrDelay >= 15 " +
      "GROUP BY Month ").registerTempTable("Delay_Month")

  val rdd_delay_month = sqlContext.sql(
    "SELECT Flights_Month.Month, DelayTotMonth, FlightTotMonth, (DelayTotMonth / FlightTotMonth) AS MonthRatio" +
      " FROM Flights_Month, " +
      "      Delay_Month" +
      " WHERE Flights_Month.Month = Delay_Month.Month").map(row => {
    val FlightsMonth = "{" + "\"FlightsMonth\":\"" +
      (    if (row(0) == "1") "January"
      else if (row(0) == "2") "February"
      else if (row(0) == "3") "March"
      else if (row(0) == "4") "April"
      else if (row(0) == "5") "May"
      else if (row(0) == "6") "June"
      else if (row(0) == "7") "July"
      else if (row(0) == "8") "August"
      else if (row(0) == "9") "September"
      else if (row(0) == "10") "October"
      else if (row(0) == "11") "November"
      else if (row(0) == "12") "December"
      else row(0))  + "\","
    val DelayTotMonth = "\"DelayTotMonth\":" + row(1) + ","
    val FlightTotMonth = "\"FlightTotMonth\":" + row(2)  + ","
    val MonthRatio = "\"MonthRatio\":" + row(3)  + "}"
    FlightsMonth + DelayTotMonth + FlightTotMonth + MonthRatio
  })

  EsSpark.saveJsonToEs(rdd_delay_month,"delay/month")
  println ("MonthDelay")


  // ****** 3º Calculamos vuelos y retrasos totales aerolinea  ******************

  sqlContext.sql(
    "SELECT Description, COUNT(*) AS FlightTotCarrier" +
      " FROM Flights_Full " +
      "GROUP BY Description ").registerTempTable("Flights_Carrier")

  sqlContext.sql(
    "SELECT Description, COUNT(*) AS DelayTotCarrier" +
      " FROM Flights_Full " +
      " WHERE ArrDelay >= 15 " +
      "GROUP BY Description ").registerTempTable("Delay_Carrier")

  val rdd_delay_carrier = sqlContext.sql(
    "SELECT Flights_Carrier.Description, DelayTotCarrier, FlightTotCarrier, " +
      " (DelayTotCarrier / FlightTotCarrier) AS RatioCarrier  " +
      " FROM Flights_Carrier, " +
      "      Delay_Carrier" +
      " WHERE Flights_Carrier.Description = Delay_Carrier.Description").map(row => {
    val FlightsCarrier = "{" + "\"FlightsCarrier\":\"" + row.getAs[String](0) + "\","
    val DelayTotCarrier = "\"DelayTotCarrier\":" + row(1) + ","
    val FlightTotCarrier = "\"FlightTotCarrier\":" + row(2)  + ","
    val RatioCarrier = "\"RatioCarrier\":" + row(3)  + "}"
    FlightsCarrier + DelayTotCarrier + FlightTotCarrier + RatioCarrier
  })

   EsSpark.saveJsonToEs(rdd_delay_carrier,"delay/carrier")
  println ("CarrierDelay")


  // ****** 4º Calculamos vuelos y retrasos totales por dia de la semana *********

    sqlContext.sql(
      "SELECT DayOfWeek, COUNT(*) AS FlightTotDayOfWeek" +
        " FROM Flights_Full " +
        "GROUP BY DayOfWeek ").registerTempTable("Flights_DayOfWeek")

    sqlContext.sql(
      "SELECT DayOfWeek, COUNT(*) AS DelayTotDayOfWeek" +
        " FROM Flights_Full " +
        " WHERE ArrDelay >= 15 " +
        "GROUP BY DayOfWeek ").registerTempTable("Delay_DayOfWeek")

    val rdd_delay_DayOfWeek = sqlContext.sql(
      "SELECT Flights_DayOfWeek.DayOfWeek, DelayTotDayOfWeek, FlightTotDayOfWeek, " +
        "(DelayTotDayOfWeek /  FlightTotDayOfWeek) AS RatioDayOfWeek " +
        " FROM Flights_DayOfWeek, " +
        "      Delay_DayOfWeek" +
        " WHERE Flights_DayOfWeek.DayOfWeek = Delay_DayOfWeek.DayOfWeek").map(row => {

      val FlightsDayOfWeek = "{" + "\"FlightsDayOfWeek\":\"" +
        (    if (row(0) == "1") "Monday"
        else if (row(0) == "2") "Thursday"
        else if (row(0) == "3") "Wednesday"
        else if (row(0) == "4") "Tuesday"
        else if (row(0) == "5") "Friday"
        else if (row(0) == "6") "Saturday"
        else if (row(0) == "7") "Sunday"
          else row(0))  + "\","
      val DelayTotDayOfWeek = "\"DelayTotDayOfWeek\":" + row(1) + ","
      val FlightTotDayOfWeek = "\"FlightTotDayOfWeek\":" + row(2)  + ","
      val RatioDayOfWeek = "\"RatioDayOfWeek\":" + row(3)  + "}"
      FlightsDayOfWeek + DelayTotDayOfWeek + FlightTotDayOfWeek + RatioDayOfWeek
    })

    EsSpark.saveJsonToEs(rdd_delay_DayOfWeek,"delay/dayOfWeek")
    println ("DayOfWeekDelay")


  // ****** 5º Calculamos vuelos y retrasos totales por hora de salida  *********

      sqlContext.sql(
        "SELECT Time, COUNT(*) AS FlightTotDepTime FROM( SELECT CASE " +
          "WHEN LENGTH(DepTime) = 3 THEN SUBSTR(CONCAT('0',DepTime),1,2)  " +
          "WHEN LENGTH(DepTime) = 2 THEN SUBSTR(CONCAT('00',DepTime),1,2) " +
          "WHEN LENGTH(DepTime) = 1 THEN SUBSTR(CONCAT('000',DepTime),1,2) " +
          " ELSE SUBSTR(DepTime,1,2) END  AS Time "+
          " FROM Flights_Full ) AS Query " +
          "GROUP BY Time ").registerTempTable("Flights_TotDepTime")


      sqlContext.sql(
        "SELECT Time, COUNT(*) AS DelayTotDepTime FROM( SELECT CASE " +
          "WHEN LENGTH(DepTime) = 3 THEN SUBSTR(CONCAT('0',DepTime),1,2)  " +
          "WHEN LENGTH(DepTime) = 2 THEN SUBSTR(CONCAT('00',DepTime),1,2) " +
          "WHEN LENGTH(DepTime) = 1 THEN SUBSTR(CONCAT('000',DepTime),1,2) " +
          " ELSE SUBSTR(DepTime,1,2) END  AS Time "+
          " FROM Flights_Full " +
          "WHERE ArrDelay >= 15 ) AS Query " +
          "GROUP BY Time ").registerTempTable("Delay_TotDepTime")


      val rdd_delay_Time = sqlContext.sql(
        "SELECT Flights_TotDepTime.Time, DelayTotDepTime, FlightTotDepTime," +
          " (DelayTotDepTime / FlightTotDepTime) AS RatioDepTime   " +
          " FROM Flights_TotDepTime, " +
          "      Delay_TotDepTime" +
          " WHERE Flights_TotDepTime.Time = Delay_TotDepTime.Time").map(row => {
        val Time = "{" + "\"Time\":\"" + row.getAs[String](0) + "\","
        val DelayTotDepTime = "\"DelayTotDepTime\":" + row(1) + ","
        val FlightTotDepTime = "\"FlightTotDepTime\":" + row(2)  + ","
        val RatioDepTime = "\"RatioDepTime\":" + row(3)  + "}"
        Time + DelayTotDepTime + FlightTotDepTime + RatioDepTime
      })

      EsSpark.saveJsonToEs(rdd_delay_Time,"delay/time")
      println("RatioDepTime")


  // ****** 6º Calculamos vuelos y retrasos totales por ruta *********

      sqlContext.sql(
        "SELECT FlightOriDest, COUNT(*) AS FlightTotOriDes" +
          " FROM (SELECT CONCAT(CONCAT(TRIM(Origin),'-'),TRIM(Dest)) AS FlightOriDest " +
          " FROM Flights_Full) AS Query " +
          "GROUP BY FlightOriDest ").registerTempTable("Flights_OriDes")

      sqlContext.sql(
        "SELECT FlightOriDest, COUNT(*) AS DelayTotOriDes" +
          " FROM (SELECT CONCAT(CONCAT(TRIM(Origin),'-'),TRIM(Dest)) AS FlightOriDest " +
          " FROM Flights_Full " +
          " WHERE ArrDelay >= 15 ) AS Query " +
          "GROUP BY FlightOriDest ").registerTempTable("Delay_OriDes")


        val rdd_delay_OriDes = sqlContext.sql(
          "SELECT Flights_OriDes.FlightOriDest, DelayTotOriDes, FlightTotOriDes, " +
            "(DelayTotOriDes / FlightTotOriDes) AS RatioTotOriDes " +
            " FROM Flights_OriDes, " +
            "      Delay_OriDes" +
            " WHERE Flights_OriDes.FlightOriDest = Delay_OriDes.FlightOriDest").map(row => {
          val Flights_OriDes = "{" + "\"Flights_OriDes\":\"" + row.getAs[String](0) + "\","
          val DelayTotOriDes = "\"DelayTotOriDes\":" + row(1) + ","
          val FlightTotOriDes = "\"FlightTotOriDes\":" + row(2)  + ","
          val RatioTotOriDes = "\"RatioTotOriDes\":" + row(3)  + "}"
          Flights_OriDes + DelayTotOriDes + FlightTotOriDes + RatioTotOriDes
        })

      EsSpark.saveJsonToEs(rdd_delay_OriDes,"delay/OriDes")



  // ****** 7º Calculamos vuelos y retrasos totales por modelo de avión *********

      sqlContext.sql(
        "SELECT model, COUNT(*) AS FlightTotModel" +
          " FROM Flights_Full " +
          "GROUP BY model ").registerTempTable("Flights_Model")

      sqlContext.sql(
        "SELECT model, COUNT(*) AS DelayTotModel" +
          " FROM Flights_Full " +
          " WHERE ArrDelay >= 15 " +
          "GROUP BY model ").registerTempTable("Delay_Model")

      val rdd_delay_Modelo = sqlContext.sql(
        "SELECT Flights_Model.model, DelayTotModel, FlightTotModel, " +
          "(DelayTotModel /  FlightTotModel) AS RatioModel " +
          " FROM Flights_Model, " +
          "      Delay_Model" +
          " WHERE Flights_Model.model = Delay_Model.model").map(row => {
        val FlightsModel = "{" + "\"FlightsModel\":\"" + row(0) + "\","
        val DelayTotModel = "\"DelayTotModel\":" + row(1) + ","
        val FlightTotModel = "\"FlightTotModel\":" + row(2)  + ","
        val RatioModel = "\"RatioModel\":" + row(3)  + "}"
        FlightsModel + DelayTotModel + FlightTotModel + RatioModel
      })

        EsSpark.saveJsonToEs(rdd_delay_Modelo,"delay/Modelo")





  sys.exit(0)


}

