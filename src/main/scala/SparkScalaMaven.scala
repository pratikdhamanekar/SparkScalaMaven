import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

object ScalaMaven {

  def main(args: Array[String]): Unit = {
    println("Welcome to my world of scala")

    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val data =  sc.textFile("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\data")
    var words = data.flatMap(line=> line.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>(x+y))
    words.foreach(println)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

     var wordsdf = words.toDF
    import org.apache.spark.sql.functions._

    val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = wordsdf.rdd
print("RDD again")
  var a = wordsdf.rdd

    a.foreach(println)
    print("sorted DF :")
    var sortedDF = wordsdf.sort(desc("_2"))

    sortedDF.show()
    //words.saveAsTextFile("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\output")
    System.out.println("Total words: " + words.count());

    println("JDBC imports")

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

    var jdbcDF = sqlContext.read.format("csv").load("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\countryDF\\").toDF("country_id","country","last_update")

    var inputDF = sqlContext.read.format("csv").load("C:\\Users\\HP\\IdeaProjects\\SparkScalaMaven\\src\\main\\resources\\countryDF\\").toDF("country_id","country","last_update")
    //println("inputdf")
    //inputDF.show()


      print("inputdf")
    inputDF.show()
    print("jdbcDF")
    jdbcDF.show()

    var joinDF = inputDF.join(jdbcDF,col("inputDF.country_id") === col("jdbcDF.country"),"inner")
      //var joinDF = inputDF.join(jdbcDF,"inputDF.country_id" == "jdbcDF.country","outer")


    joinDF.show

  }
}
