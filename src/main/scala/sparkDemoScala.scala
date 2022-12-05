
import jdk.nashorn.internal.runtime.regexp.RegExp
import org.apache.spark.sql.functions.{approx_count_distinct, array_contains, avg, col, count, countDistinct, current_date, datediff, expr, format_number, mean, regexp_extract, round, row_number, stddev, to_date}
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import scala.io.Source


object sparkDemoScala extends App{

  //creating a spark session
  val spark=SparkSession.builder()
    .appName("Apache Spark DataTransformations")
    .config("spark.master", "local")
    .getOrCreate()

  //API for reading a database from my computer
  def readDF(filename:String, formatDB:String)=spark.read
    .format(formatDB)
    .option("inferSchema", "true")
    .load(s"src/main/resources/$filename")

  val bandsDF=readDF("bands.json", "json")
  val guitarPlayersDF=readDF("guitarPlayers.json", "json")
  val guitarsDF0=readDF("guitars.json", "json")


    ""

  //Showing the Tables on the Console
  /*
  bandsDF.show()
  guitarPlayersDF.show()
  guitarsDF.show()
*/
  //Taking the  first 5 lines of the gitarsDF as array of rows
  guitarsDF0.take(5).foreach(println)

  //Creating a new DataFrame, which details the hobbies of the guitarist
  //Creating a Schema in case you save it and load it with this schema
  val hobbySchema = StructType(Array(
    StructField("id", StringType),
    StructField("name", StringType),
    StructField("hobby", StringType),
    StructField("spendingperyear", StringType)))
/*
  val hobbiesPlayers=spark.read
    .format("csv")
    .schema(hobbySchema)
    .load("src/main/resources/hobbies.csv")
*/
  val hobbies=Seq(
    (0, "Jimmy Page", "sailing", 42500),
    (0, "Angus Young", "video games", 15000),
    (0, "Eric Clapton", "cars", 265020),
    (0, "Kirk Hammett", "travelling", 56400),
    (0, "Frank Gambale", "auto racing", 19300))

  //creating the hobbies DataFrame by auto-inferring the schema
  val hobbiesDFAutoInferred=spark.createDataFrame(hobbies)

  //create hobbiesDF with Implicits
  import spark.implicits._
  val hobbiesDFwithImplicits=hobbies.toDF("id", "name", "hobby", "spendingperyear")

  //##########################  EXPLORE the guitarsDF0 dataframe  ################################x

  //1.) check if we have null values

  guitarsDF0.where(
    col("id").isNull ||
    col("Brand").isNull ||
    col("GuitarType").isNull ||
    col("Model").isNull ||
    col("Origin").isNull ||
    col("ProductionStartDate").isNull ||
    col("WorldwideGrossProfit(USD)").isNull
    ).show()

  //same result with one line:
  val filteredGuitarsDF=guitarsDF0.where(guitarsDF0.columns.map(x=>col(x).isNull).reduce(_||_))



  //2. Replace the null value with the average of WorldwideGrossProfit(USD)

  val avgWWGProfitDouble=guitarsDF0.select(avg(col("WorldwideGrossProfit(USD)")))
  val avgWWGProfitInt=avgWWGProfitDouble.first.getDouble(0).toInt  //first return a row objext and we convert Double to Int
  val guitarsDF00=guitarsDF0.na.fill(Map(
    "WorldwideGrossProfit(USD)" -> avgWWGProfitInt
  ))


  //3. Check how many rows we have
  val NumberOfRows=guitarsDF00.select(count("*"))

  //4.Check distinct values in columns "Origin", "Brand" and "ProductionStartDate"
  guitarsDF00.select(countDistinct(col("Origin")), countDistinct(col("Brand")),
    countDistinct(col("ProductionStartDate")))
          //we use the following when having big data, it doesn't count the row one by one:
  guitarsDF00.select(approx_count_distinct(col("Origin")), approx_count_distinct(col("Brand"))
  , approx_count_distinct(col("ProductionStartDate")))

  //It seems we have different DATE FORMATS, let's check it
  val dataformatsDF=guitarsDF00.select("ProductionStartDate").distinct()

  //CREATING A GUITARS DATAFRAME WHERE THE DATE FORMAT IS THE SAME IN EVERY ROW
    //We have two different date type so first we convert the first one to DateType
  val guitarsDF2=guitarsDF00.select(col("id"), col("Model"), col("Brand"), col("GuitarType"), col("Origin"),
    to_date(col("ProductionStartDate"),"yyyy-MM-dd")
      .as("ProductionStartDate"), col("WorldwideGrossProfit(USD)"))
    //converting the second one
  val guitarsDF3=guitarsDF00.select(col("id"), col("Model"), col("Brand"), col("GuitarType"), col("Origin"),
    to_date(col("ProductionStartDate"),"dd-MM-yyyy")
      .as("ProductionStartDate"), col("WorldwideGrossProfit(USD)"))

  //Creating a new Dataframe by make union of the two dataframe with the same DateType
  val guitarsDF=guitarsDF2.where(col("ProductionStartDate").isNotNull).drop()
    .unionByName(guitarsDF3.where(col("ProductionStartDate").isNotNull).drop())

  //5. Check the min and max yearly profit
  val minProfitDF=guitarsDF.select(functions.min(col("WorldwideGrossProfit(USD)")))
  val maxProfitDF=guitarsDF.select(functions.max(col("WorldwideGrossProfit(USD)")))
        //Showing the row where this profit can be found
  guitarsDF.filter(guitarsDF("WorldwideGrossProfit(USD)")==="337822813")

  //6. Check the total and average profit in the industry, and the standard deviation regarding profit
  val totalProfit=guitarsDF.select(functions.sum(col("WorldwideGrossProfit(USD)")))
  val averageProfit=guitarsDF.select(format_number(avg(col("WorldwideGrossProfit(USD)")),2))

  val meanStddevofProfit=guitarsDF.select(
    format_number(mean(col("WorldwideGrossProfit(USD)")),2).as("MEAN"),  //same as avg
    format_number(stddev(col("WorldwideGrossProfit(USD)")),2).as("STD")
  )



  //#################    BASIC DF OPERATIONS (Filtering, Distinct value, Union, ColumnType coversion, Joins)   ##########################


  //1. Create a new dataframe from the guitarsDF with columns: Brand, Guitar model, Origin, Gross yearly profit and
  //rename them with different technics and convert the WorldWideYearly profit to EUR formatting 2 decimal digits

  val newGuitarDataFrame=guitarsDF.select(
    expr("Brand").as("Manufacturer"),
    $"Model".as("Type"),
    'Origin.as("Country"),
    format_number((col("WorldwideGrossProfit(USD)")/0.91), 2).as("Gross yearly profit(EUR)")

  )

  //2. Filtering Columns
  //Guitars not from USA
  val notAmericanGuitars=guitarsDF.filter(col("Origin") =!= "USA")
  val americanGuitars=guitarsDF.where(col("Origin") === "USA")
  //Ibanez guitars
  val ibanezGuitars=guitarsDF.filter("Brand='Ibanez'")
  //Ibanez guitars issued after 1990
  val ibanezGuitarsfrom1990=guitarsDF.filter("Brand='Ibanez' and ProductionStartDate > '1990-01-01'")
  //filtering with regex on "Electric" keyword
  val regexString = "Electric|electric|ELECTRIC"
  val electricDF=guitarsDF.select(
    col("GuitarType"),
    col("Brand"),
    regexp_extract(col("GuitarType"), regexString, 0).as("regex_extract"))
    .where(col("regex_extract")=!= "").drop(col("regex_extract"))


  //3. Distinct Values, Check what countries the guitar manufacturer are from
  val allCountriesDF=guitarsDF.select("Origin").distinct()
  allCountriesDF

  //4. Concat guitars and guitars2 datasheets

  //We have to convert guitar2 datefield from string to datetype
  val guitar2Schema = StructType(Array(
    StructField("id", StringType),
    StructField("Model", StringType),
    StructField("Brand", StringType),
    StructField("GuitarType", StringType),
    StructField("Origin", StringType),
    StructField("ProductionStartDate", DateType),
    StructField("WorldwideGrossProfit(USD)", IntegerType)
  ))
  val allGuitars=guitarsDF.union(spark.read.schema(guitar2Schema).json("src/main/resources/guitars2.json"))
  allGuitars

  //5. Check the Profit and Profit on average by Origin in descending order

  val aggregationbyProfit=guitarsDF
    .groupBy(col("Origin"))
    .agg(
      count("*").as("Guitar_models"),
      avg("WorldwideGrossProfit(USD)").as("Avg_WorldWideGrossProfit(USD)")
    ).orderBy(col("Avg_WorldWideGrossProfit(USD)").desc)

  //6. Get the age of the guitars
  val guitarsAge0=guitarsDF
    .withColumn("Today", current_date())
    .withColumn("Guitar_Age_Year", round(datediff(col("Today"),
      col("ProductionStartDate"))/365))
  val guitarsAge=guitarsAge0.drop(col("Today"))
  guitarsAge.orderBy(col("Guitar_Age_Year").desc)

  //7. JOINS
  //Inner join on the basis of the ID regarding bandsDF , guitarPlayerDF and guitarsDF
  //"Inner" combines all the rows which matches the expression
  //Joining bandsDF and guitarPlayerDF
  val guitarPlayersBandDF=guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  //second method with dropping the dupe column
  val guitarPlayersBandDF2=guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "inner")
  val guitarPlayersBandDF2Final=guitarPlayersBandDF2.drop(bandsDF.col("id"))

  //Joining bandsDF , guitarPlayerDF and guitarsDF into one
  val guitarGuitarPlayersBandDF=guitarPlayersBandDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)"))

  guitarGuitarPlayersBandDF

  //#####################   Converting a DATAFRAME to a DATASET  ##############################

  //Datasets are typed Dataframes, and distrubuted collection of JVM object while Dataframes are distrubuted collection of rows
  //Spark allows us to have more type information on a Dataframe


  val guitarPlayersSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("guitars", ArrayType(IntegerType)),
    StructField("band", IntegerType)
  ))

  val bandsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("hometown", StringType),
    StructField("year", IntegerType)
  ))

  val guitarPlayersDF2=spark.read.format("json").schema(guitarPlayersSchema).load("src/main/resources/guitarPlayers.json")
  val bandsDF2=spark.read.format("json").schema(bandsSchema).load("src/main/resources/bands.json")


  case class GuitarPlayer(id: Int, name:String, guitars: Seq[Int], band:Int)  //pl [1,5] típusa= Seq[Int]
  case class Band(id:Int, name:String, hometown:String, year: Int)

  import spark.implicits._

  val guitarPlayersDS=guitarPlayersDF2.as[GuitarPlayer]
  val bandDS=bandsDF2.as[Band]

  //do some join (a tuple will contain the two datatable we joined)
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)]=guitarPlayersDS.joinWith(bandDS,
    guitarPlayersDS.col("band")===bandDS.col("id"), "inner")


  //#########################################   SPARK SQL   ###############################################

  //Spark SQL is a core functionality for Spark
  //In Spark  SQL is an abstraction over DataFrames
  //Support for SQL is done in two ways: in expressions and in the Spark Shell

  //regular Dataframe API selecting USA guitars
  val guitarsDFnew=spark.read.option("inferSchema", "true").json("src/main/resources/guitar3.json")
  val USAguitarsDF=guitarsDFnew.where(col("Origin")==="USA")

  //1. same in Spark SQL
  guitarsDFnew.createOrReplaceTempView("guitars")  //it creates alias in spark we refer to it as table /*

  val americanGuitarsDF = spark.sql(
    """
      |select Brand from guitars where Origin='USA'
    """.stripMargin)

  //2.Count how many giutars were issued fater 1990-01-01
  val guitarsAfter1990= spark.sql(
    """
      |select count(*)
      |from guitars
      |where ProductionStartDate > "1990-01-01"
      |""".stripMargin)



  //3. Showing which country had the highest WorldwideGrossProfit(USD) from guitars issued between 1960 and 2000
  val averageGrossProfit=spark.sql(
    """
      |select Origin, sum(WorldwideGrossProfit_USD_) WorldWideGrossProfit
      |from guitars
      |where ProductionStartDate > "1960-01-01" and ProductionStartDate < "2000-01-01"
      |group by Origin
      |order by WorldWideGrossProfit desc
      |limit 1
      |""".stripMargin
  )


  //#########################################   RDD (Resilient Distributed Dataset )   ###############################################

  /*
  RDD is quite similar to datasets: distributed typed collection of JVM objects as Dataset, but RDD is the first
  underlying first citizen of SPARK, when we work with DF or DS, RDD is beeing proccessed behind the scene, RDD is
  the building block of big data, it is partitioned, and we can controll how it is done. If we want to boost up the
  performance and we need to fine tune the operation on the distributed data, we should use RDD, but normally it is enough
  to use DataFrame or Dataset.
   */

  //Reading RDD from the band file


  val bandsRDD=bandsDF2
    .select(col("id"), col("name"),
      col("hometown"), col("year"))
    .where(col("id").isNotNull and col("name").isNotNull)
    .as[Band] //creating dataset
    .rdd  //creating rdd


  //RDD to DF
  val BandRDDtoDF=bandsRDD.toDF("id", "name", "hometown", "year")

  //RDD to DS
  val BandRDDtoDS=spark.createDataset(bandsRDD)
  BandRDDtoDS.show()

  //Some basic operations on RDD
  
  val MetallicaRDD=bandsRDD.filter(_.name=="Metallica")
  MetallicaRDD.collect().foreach(println)

  val bandNames=bandsRDD.map(_.name).distinct()
  bandNames.collect().foreach(println) //or
  bandNames.toDF.show()

  val bandsFormedAfter1980=bandsRDD.filter(band=>band.year > 1980)
  bandsFormedAfter1980.toDF.show()

































}
