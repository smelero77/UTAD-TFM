import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source


object ScalaApp extends App{
  //http://stat-computing.org/dataexpo/2009/1987.csv.bz2
 /* try {
    val src = scala.io.Source.fromURL("http://stat-computing.org/dataexpo/2009/1987.csv.bz2")
    val out = new java.io.FileWriter("home/sergio/TFM_Ficheros/1987.csv.bz2")
    out.write(src.mkString)
    out.close
  } catch {
    case e: java.io.IOException => "error occured"
  }*/
  /**
    * downloads a url (file) to a desired file name
    */
  def downloadFile(url: URL, filename: String) {
    commonOp(url2InputStream(url), filename)
  }

  /**
    * common method for writing data from an inputstream to an outputstream
    */
  def commonOp(is: InputStream, filename: String) {
    val out: OutputStream = file2OutputStream(filename)
    try {
      deleteFileIfExists(filename)
      copy(is, out)
    } catch {
      case e: Exception => println(e.printStackTrace())
    }

    out.close()
    is.close()

  }

  /**
    * copies an inputstream to an  outputstream
    */
  def copy(in: InputStream, out: OutputStream) {
    val buffer: Array[Byte] = new Array[Byte](1024)
    var sum: Int = 0
    Iterator.continually(in.read(buffer)).takeWhile(_ != -1).foreach({ n => out.write(buffer, 0, n); (sum += buffer.length); println(sum + " written to output "); })
  }

  /**
    * handling of bzip archive files
    */
  def unzipFile(fn: String, outputFileName: String) {
    val in = new BZip2CompressorInputStream(new FileInputStream(fn))
    commonOp(in, outputFileName)

  }
/







 /* //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf     // nueva configuracion
  sparkConfig.setMaster("local[*]")   // set master que en este caso indica que es cluster local
  sparkConfig.setAppName("EjercicioSpark")      // nombre de la aplicacion
  val sparkContext = new SparkContext(sparkConfig)  //contexto de spark

  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext) // contexto de Spark SQL

  
  val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("OlympicAthletes.csv")

  df.registerTempTable("Medallas")

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

