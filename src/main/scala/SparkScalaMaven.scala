import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer



object SparkScalaMaven {



  def main(args: Array[String]): Unit = {
    println("Welcome to my world of scala")
    val logger = LogUtils.getLogger(this.getClass.getSimpleName)

    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)
    logger.info(s" Logger Info Spark context started: $sc")
    val data =  sc.textFile("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\data")
    var words = data.flatMap(line=> line.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>(x+y))
    //words.foreach(println)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

     var wordsdf = words.toDF("Word","Count")
    //wordsdf.show()
    import org.apache.spark.sql.functions._

/*    val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = wordsdf.rdd
print("RDD again")
  var a = wordsdf.rdd

    a.foreach(println)
    print("sorted DF :")
    var sortedDF = wordsdf.sort(desc("_2"))

    sortedDF.show()
    //words.saveAsTextFile("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\output")
    System.out.println("Total words: " + words.count());*/

   /* println("JDBC imports")

     /* val jdbcDF = sqlContext.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "country")
      .option("user", "root")
      .option("password", "password")
      .load()*/

    //jdbcDF.show()
    //jdbcDF.write.format("csv").save("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\countryDF")

    //var jdbcDF = sqlContext.read.format("csv").load("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\countryDF\\").toDF("country_id","country","last_update")

    //var inputDF = sqlContext.read.format("csv").load("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\countryDF\\").toDF("country_id","country","last_update")
    //println("inputdf")
    //inputDF.show()


      //print("inputdf")
    //inputDF.show()
    //print("jdbcDF")
   // jdbcDF.show()

    var joinDF = inputDF.join(jdbcDF,col("inputDF.country_id") === col("jdbcDF.country"),"inner")
      //var joinDF = inputDF.join(jdbcDF,"inputDF.country_id" == "jdbcDF.country","outer")


    joinDF.show*/

    //Filter DF
    var filtered_wordsdf = wordsdf.filter(!($"Word".contains("\"")) && !($"Word".contains("}")))

    filtered_wordsdf.show()
//With Column
    var added_column = filtered_wordsdf.withColumn("Range", when($"count" <= 2,"min" ).when($"count"<2 && $"count" <=4,"AVG").otherwise("high"))

    //added_column.show()
    filtered_wordsdf.withColumn("concat_column",concat($"word",lit("-"), $"count") ).show()

    //map on dataframes
    //filtered_wordsdf.map(x=> x.getString(1)).show()
    //filtered_wordsdf.map(x=> x.getInt(1)).show()
    //filtered_wordsdf.map(x=> x.getString(1)).foreach()

/*

   println("sum of array")
   val arr = Array(1 to 10)
    val rdd = sc.parallelize(arr)
   import org.apache.spark.rdd

    val ans = rdd.foreach(x=>{
      var ite = x.iterator
      var sum = 0
      while(ite.hasNext)
      {
        println("inside while")
        sum += ite.next()


      }
      print("sum:"+ sum)
    })
*/

    /*var sum = 0
    var it = rdd.it
    while(it.hasNext)
      {
        println("inside while")
        sum += it.next()
      }
    print("sum without foreach:"+ sum)*/


     val arrstring = Array("A","A","B","B")
     val rddString = sc.parallelize(arrstring)
     var grpd =rddString.groupBy(x => x.charAt(0))
       println("grp : "+grpd.collect())
      grpd.foreach(x => {

       var ite = x._2.iterator

        println("x._1 ->>"+ x._1)
        println("x._2 ->>"+ x._2)

       var sum =0
       var count = 0
       while(ite.hasNext)
         {
             println("inside while")
             count +=1
             ite.next()

         }
         println(s"count of"+ x._1+ " "+ count)
     })

    println("struct type dynamically")
    val data1 = Seq(
      Row("dog", "12"),
      Row("cAT", "6"),
      Row("ELEPHENT", "70")
    )
    val rdddata1 = sc.parallelize(data1)


    val str = new StructType()
    val schemaString = "name\tage"
    var count=0
    val splitString = schemaString.split("\t")
    var list =   new ListBuffer[StructField]()
    var it = splitString.iterator
    while(it.hasNext)
      {

        count +=1
        //str.add(StructField(it.next(), StringType, true))
        list += StructField(it.next(), StringType, true)
      }


    val schema = StructType(list)
    print("schema :"+ list)

    var structDF = sqlContext.createDataFrame(rdddata1,schema)
    structDF.show()


    var schema1 = new StructType()
    schemaString.split("\t").foreach(x=> {

      schema1 = schema1.add(StructField(x,StringType,true))
    })

    val newstructDF = sqlContext.createDataFrame(rdddata1,schema1)
    newstructDF.show()
    println("this was new df created")


    //val (dfnew1 : DataFrame, DF2 : DataFrame)= creteDFs(sc)
  }


  def creteDFs(sc:SparkContext) = {
    val data =  sc.textFile("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\data")

    val data1= sc.textFile("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\data")

    (data,data1)
  }


  }



