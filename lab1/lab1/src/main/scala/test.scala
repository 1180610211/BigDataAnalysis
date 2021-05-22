import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Locale

object test {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
//    val sc = new SparkContext(conf)
//
//    val rdd = sc.textFile("hdfs://hadoop100:9000/user/hadoop/data.txt")
//      //      .map(_.split("\\|")(11)).distinct()
//      .filter(_.split("\\|")(11).equals("?"))
//    print(rdd.count())
    //    rdd.collect().foreach(println)
    //            println(rdd.first())

//    sc.stop()
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("Basic")
    //    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    import spark.implicits._
    //
    //    val df=spark.read.format("csv")
    //      .option("header",false)
    //      .option("sep","|")
    //      .load("hdfs://hadoop100:9000/user/hadoop/data.txt")
    //
    //    println(df.describe().show())
    //
    //    spark.close()


    val format3 = new SimpleDateFormat("MMMM dd,yyyy",Locale.US)
    val date = format3.parse("May 13,1972")
    print(date)
  }
}
