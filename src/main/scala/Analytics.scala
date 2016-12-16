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
    .set("es.net.http.auth.pass", "sergio77")


  val sparkContext = new SparkContext(sparkConfig)  //contexto de spark

  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext) // contexto de Spark SQL

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
      "WHERE TRIM(Flights_Planedata_Carriers_AirportO.Dest) = TRIM(Airports.iata)  LIMIT 10000"
  )

    //.registerTempTable("Flights_Full")


  // Cálculo de las métricas

  // ****** 1º Calculamos vuelos y retrasos totales por año  ******************
/*
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
     "SELECT Flights_Year.Year, DelayTotYear, FlightTotYear " +
      " FROM Flights_Year, " +
      "      Delay_Year" +
      " WHERE Flights_Year.Year = Delay_Year.Year").map(row => {
     val FlightsYear = "{" + "\"FlightsYear\":\"" + row(0) + "\","
     val DelayTotYear = "\"DelayTotYear\":\"" + row.getAs[Long](1) + "\","
     val FlightTotYear = "\"FlightTotYear\":\"" + row.getAs[Long](2)  + "\"}"
     FlightsYear + DelayTotYear + FlightTotYear
   })

  EsSpark.saveJsonToEs(rdd_delay_year,"delays/year")
*/
  //************************************************************************
/*
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
    "SELECT Flights_Month.Month, DelayTotMonth, FlightTotMonth " +
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
    val DelayTotMonth = "\"DelayTotMonth\":\"" + row.getAs[Long](1) + "\","
    val FlightTotMonth = "\"FlightTotMonth\":\"" + row.getAs[Long](2)  + "\"}"
    FlightsMonth + DelayTotMonth + FlightTotMonth
  })

  EsSpark.saveJsonToEs(rdd_delay_month,"delays/month")
*/
  //************************************************************************

  // ****** 3º Calculamos vuelos y retrasos totales aerolinea  ******************
/*
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
    "SELECT Flights_Carrier.Description, DelayTotCarrier, FlightTotCarrier " +
      " FROM Flights_Carrier, " +
      "      Delay_Carrier" +
      " WHERE Flights_Carrier.Description = Delay_Carrier.Description").map(row => {
    val FlightsCarrier = "{" + "\"FlightsCarrier\":\"" + row.getAs[Int](0) + "\","
    val DelayTotCarrier = "\"DelayTotCarrier\":\"" + row.getAs[Long](1) + "\","
    val FlightTotCarrier = "\"FlightTotCarrier\":\"" + row.getAs[Long](2)  + "\"}"
    FlightsCarrier + DelayTotCarrier + FlightTotCarrier
  }).foreach(println)

  //EsSpark.saveJsonToEs(rdd_delay_carrier,"delays/carrier")
*/
  //************************************************************************

  // ****** 4º Calculamos vuelos y retrasos totales por dia de la semana *********
/*
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
      "SELECT Flights_DayOfWeek.DayOfWeek, DelayTotDayOfWeek, FlightTotDayOfWeek " +
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
      val DelayTotDayOfWeek = "\"DelayTotDayOfWeek\":\"" + row.getAs[Long](1) + "\","
      val FlightTotDayOfWeek = "\"FlightTotDayOfWeek\":\"" + row.getAs[Long](2)  + "\"}"
      FlightsDayOfWeek + DelayTotDayOfWeek + FlightTotDayOfWeek
    }).foreach(println)

    //EsSpark.saveJsonToEs(rdd_delay_DayOfWeek,"delays/dayOfWeek")
*/
  //***************************************************************************

  // ****** 5º Calculamos vuelos y retrasos totales por hora de salida  *********
  /*
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
        "SELECT Flights_TotDepTime.Time, DelayTotDepTime, FlightTotDepTime " +
          " FROM Flights_TotDepTime, " +
          "      Delay_TotDepTime" +
          " WHERE Flights_TotDepTime.Time = Delay_TotDepTime.Time").map(row => {
        val Time = "{" + "\"Time\":\"" + row.getAs[Int](0) + "\","
        val DelayTotDepTime = "\"DelayTotDepTime\":\"" + row.getAs[Long](1) + "\","
        val FlightTotDepTime = "\"FlightTotDepTime\":\"" + row.getAs[Long](2)  + "\"}"
        Time + DelayTotDepTime + FlightTotDepTime
      }).foreach(println)
*/
      //EsSpark.saveJsonToEs(rdd_delay_Time,"delays/time")

  //***************************************************************************
/*
  // ****** 6º Calculamos vuelos y retrasos totales por ruta *********

      sqlContext.sql(
        "SELECT FlightOriDest, COUNT(*) AS FlightTotOriDes" +
          " FROM (SELECT CONCAT(CONCAT(TRIM(Origin),' '),TRIM(Dest)) AS FlightOriDest " +
          " FROM Flights_Full) AS Query " +
          "GROUP BY FlightOriDest ").registerTempTable("Flights_OriDes")

      sqlContext.sql(
        "SELECT FlightOriDest, COUNT(*) AS DelayTotOriDes" +
          " FROM (SELECT CONCAT(CONCAT(TRIM(Origin),' '),TRIM(Dest)) AS FlightOriDest " +
          " FROM Flights_Full " +
          " WHERE ArrDelay >= 15 ) AS Query " +
          "GROUP BY FlightOriDest ").registerTempTable("Delay_OriDes")


        val rdd_delay_OriDes = sqlContext.sql(
          "SELECT Flights_OriDes.FlightOriDest, DelayTotOriDes, FlightTotOriDes " +
            " FROM Flights_OriDes, " +
            "      Delay_OriDes" +
            " WHERE Flights_OriDes.FlightOriDest = Delay_OriDes.FlightOriDest").map(row => {
          val Time = "{" + "\"Time\":\"" + row.getAs[Int](0) + "\","
          val DelayTotOriDes = "\"DelayTotOriDes\":\"" + row.getAs[Long](1) + "\","
          val FlightTotOriDes = "\"FlightTotOriDes\":\"" + row.getAs[Long](2)  + "\"}"
          Time + DelayTotOriDes + FlightTotOriDes
        }).foreach(println)

  //EsSpark.saveJsonToEs(rdd_delay_OriDes,"delays/OriDes")
*/
  //***************************************************************************






  val rdd_completo = df_full.map(row => {
    val FlightsYear = "{" + "\"FlightsYear\":\"" + row.getAs[Int](0) + "\","
    val FlightsMonth = "\"FlightsMonth\":\"" +
      (    if (row(1) == "1") "January"
      else if (row(1) == "2") "February"
      else if (row(1) == "3") "March"
      else if (row(1) == "4") "April"
      else if (row(1) == "5") "May"
      else if (row(1) == "6") "June"
      else if (row(1) == "7") "July"
      else if (row(1) == "8") "August"
      else if (row(1) == "9") "September"
      else if (row(1) == "10") "October"
      else if (row(1) == "11") "November"
      else if (row(1) == "12") "December"
      else row(0))  + "\","
    val FlightsDayofMonth = "\"FlightsDayofMonth\":\"" + row.getAs[Int](2) + "\","
    val FlightsDayOfWeek = "\"FlightsDayOfWeek\":\"" +
      (    if (row(3) == "1") "Monday"
      else if (row(3) == "2") "Thursday"
      else if (row(3) == "3") "Wednesday"
      else if (row(3) == "4") "Tuesday"
      else if (row(3) == "5") "Friday"
      else if (row(3) == "6") "Saturday"
      else if (row(3) == "7") "Sunday"
      else row(0))  + "\","
    val FlightsDepTime = "\"FlightsDepTime\":\"" + row.getAs[Int](4) + "\","
    val FlightsCRSDepTime = "\"FlightsCRSDepTime\":\"" + row.getAs[Int](5) + "\","
    val FlightsArrTime = "\"FlightsArrTime\":\"" + row.getAs[Int](6) + "\","
    val FlightsCRSArrTime = "\"FlightsCRSArrTime\":\"" + row.getAs[Int](7) + "\","
    val FlightsActualElapsedTime = "\"FlightsActualElapsedTime\":\"" + row.getAs[Int](11) +"\","
    val FlightsCRSElapsedTime = "\"FlightsCRSElapsedTime\":\"" + row.getAs[Int](12) +"\","
    val FlightsAirTime = "\"FlightsAirTime\":\"" + row.getAs[Int](13) + "\","
    val FlightsArrDelay = "\"FlightsArrDelay\":\"" + row.getAs[Int](14) + "\","
    val FlightsDepDelay = "\"FlightsDepDelay\":\"" + row.getAs[Int](15) + "\","
    val FlightsOrigin = "\"FlightsOrigin\":\"" + row.getAs[String](16) + "\","
    val FlightsDest = "\"FlightsDest\":\"" + row.getAs[String](17) + "\","
    val FlightsDistance = "\"FlightsDistance\":\"" + row.getAs[Int](18) +"\","
    val FlightsTaxiIn = "\"FlightsTaxiIn\":\"" + row.getAs[Int](19) + "\","
    val FlightsTaxiOut = "\"FlightsTaxiOut\":\"" + row.getAs[Int](20) + "\","
    val FlightsCancelled = "\"FlightsCancelled\":\"" + row.getAs[Int](21) + "\","
    val FlightsCancellationCode = "\"FlightsCancellationCode\":\"" + row.getAs[Int](22) +"\","
    val FlightsDiverted = "\"FlightsDiverted\":\"" + row.getAs[Int](23) +"\","
    val FlightsCarrierDelay = "\"FlightsCarrierDelay\":\"" + row.getAs[Int](24) +"\","
    val FlightsWeatherDelay = "\"FlightsWeatherDelay\":\"" + row.getAs[Int](25) +"\","
    val FlightsNASDelay = "\"FlightsNASDelay\":\"" + row.getAs[Int](26) +"\","
    val FlightsSecurityDelay = "\"FlightsSecurityDelay\":\"" + row.getAs[Int](27) + "\","
    val FlightsLateAircraftDelay = "\"FlightsLateAircraftDelay\":\"" + row.getAs[String](28) +"\","
    val Planedatamanufacturer = "\"Planedatamanufacturer\":\"" + row.getAs[String](29) +"\","
    val Planedatamodel = "\"Planedatamodel\":\"" + row.getAs[String](30) + "\","
    val Planedataaircraft_type = "\"Planedataaircraft_type\":\"" + row.getAs[String](31) +"\","
    val Planedataengine_type = "\"Planedataengine_type\":\"" + row.getAs[String](32) + "\","
    val Planedatayear = "\"Planedatayear\":\"" + row.getAs[Int](33) + "\","
    val CarriersDescription = "\"CarriersDescription\":\"" + row.getAs[String](34) + "\","
    val airporto = "\"airporto\":\"" + row.getAs[String](35) + "\","
    val cityo = "\"cityo\":\"" + row.getAs[String](36) + "\","
    val stateo = "\"stateo\":\"" + row.getAs[String](37) +"\","
    val coordenadao = "\"coordenadao\":\"" + row.getAs[String](38) + "," + row.getAs[String](39) +"\","
    val airportd = "\"airportd\":\"" + row.getAs[String](40) +"\","
    val cityd = "\"cityd\":\"" + row.getAs[String](41) +"\","
    val stated = "\"stated\":\"" + row.getAs[String](42) + "\","
    val retraso = "\"retraso\":\"" + (if(row(14).toString > "15") "1" else "0") + "\","
    val vuelo = "\"vuelo\":\"" + "1" + "\","
    val coordenadad = "\"coordenadad\":\"" + row.getAs[String](43) + "," + row.getAs[String](44) + "\"}"
    FlightsYear + FlightsMonth + FlightsDayofMonth + FlightsDayOfWeek + FlightsDepTime + FlightsCRSDepTime +
      FlightsArrTime + FlightsCRSArrTime + FlightsActualElapsedTime + FlightsCRSElapsedTime + FlightsAirTime +
      FlightsArrDelay + FlightsDepDelay + FlightsOrigin + FlightsDest +  FlightsDistance + FlightsTaxiIn +
      FlightsTaxiOut + FlightsCancelled + FlightsCancellationCode + FlightsDiverted + FlightsCarrierDelay +
      FlightsWeatherDelay + FlightsNASDelay + FlightsSecurityDelay +  FlightsLateAircraftDelay +  Planedatamanufacturer +
      Planedatamodel +  Planedataaircraft_type + Planedataengine_type + Planedatayear + CarriersDescription +
      airporto + cityo + stateo + coordenadao + airportd + cityd + stated +  retraso + vuelo + coordenadad

  })
rdd_completo.foreach(println)
rdd_completo.filter(_.nonEmpty)
  .map(_.replaceAll(",NA,NA,", ",\"A\",\"A\","))
  .map(_.replaceAll(", ", " "))
  .map(_.replaceAll("w,I", "w I"))
  .map(_.replaceAll("Reading Muni,Gen", "Reading Muni Gen"))
  .map(_.replaceAll("Lawrence County Airpark,Inc", "Lawrence County Airpark Inc"))
 EsSpark.saveJsonToEs(rdd_completo,"retrasos/query1")
/*
 val make = if (row1.toLowerCase == "tesla") "S" else row1
    Row(row(0),make,row(2))
  }).collect().foreach(println)


  dataframe.map(row => {
    val row1 = row.getAs[String](1)
    val make = if (row1.toLowerCase == "tesla") "S" else row1
    Row(row(0),make,row(2))
  }).collect().foreach(println)

*/



  sys.exit(0)


}

