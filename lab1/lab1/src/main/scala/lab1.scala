import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, expr, isnan}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Locale

object lab1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("lab1")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val originalData = sc.textFile("hdfs://hadoop100:9000/user/hadoop/data.txt")

    //对原始数据进行探索，预先查看一下所有的职业类别
    //Manager
    //teacher
    //accountant
    //programmer
    //farmer
    //doctor
    //writer
    //artist

//        val userCareer = originalData.map(item => {
//          val fields = item.split("\\|")
//          fields(10)
//        }).distinct().collect()
//        userCareer.foreach(item => println("\"" + item + "\""))

    // 1、分层抽样
    val rate = 0.05
    val seed = 99L
    val sampleRateMap: Map[String, Double] =
      List("Manager", "teacher", "accountant", "programmer", "farmer", "doctor", "writer", "artist")
        .map((_, rate)).toMap

    val sampledData: RDD[Array[String]] = originalData.map(item => {
      val fields = item.split("\\|")
      (fields(10), fields)
    }).sampleByKey(false, sampleRateMap, seed).map(_._2)

    // 保存为data_Sample
    println(sampledData.count()) //239066
    sampledData
      .map(_.mkString("|"))
      .coalesce(1).saveAsTextFile("hdfs://hadoop100:9000/user/hadoop/data_Sample")

    // 2、数据过滤
    val filteredData = sampledData.filter(fields => {
      val longitude = fields(1).toDouble
      val latitude = fields(2).toDouble
      //longitude的有效范围为[8.1461259, 11.1993265]，
      //latitude的有效范围为[56.5824856, 57.750511]
      longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511
    })

    // 保存为data_Filter
    println(filteredData.count()) //478174
    filteredData
      .map(_.mkString("|"))
      .coalesce(1).saveAsTextFile("hdfs://hadoop100:9000/user/hadoop/data_Filter")

    filteredData.cache()
    filteredData.count()

    // 3.0、计算rating属性的最大值和最小值
    //一轮map-reduce
    val (min, max) = filteredData.filter(fields => !fields(6).equals("?"))
      .map(fields => (fields(6).toDouble, fields(6).toDouble))
      .reduce((item1, item2) =>
        (if (item1._1 < item2._1) item1._1 else item2._1,
          if (item1._2 < item2._2) item2._2 else item1._2)
      )
    println("min:" + min)
    println("max:" + max)

    //    // 验证一下正确性
    //    println(filteredData.filter(fields => !fields(6).equals("?"))
    //      .map(fields => fields(6).toDouble).min())
    //    println(filteredData.filter(fields => !fields(6).equals("?"))
    //      .map(fields => fields(6).toDouble).max())

    // 3.1、数据格式转换与数值型数据归一化
    // 1991-04-14 1992/03/25 June 6,1978
    val filteredData2 = filteredData.map(fields => {
      var reviewDate = fields(4)
      var temperature = fields(5)
      var userBirthday = fields(8)

      val format1 = new SimpleDateFormat("yyyy-MM-dd")
      val format2 = new SimpleDateFormat("yyyy/MM/dd")
      val format3 = new SimpleDateFormat("MMMM d,yyyy", Locale.US)


      if (reviewDate.contains("/")) {
        val date = format2.parse(reviewDate)
        reviewDate = format1.format(date)
      } else if (reviewDate.contains(",")) {
        val date = format3.parse(reviewDate)
        reviewDate = format1.format(date)
      }

      if (userBirthday.contains("/")) {
        val date = format2.parse(userBirthday)
        userBirthday = format1.format(date)
      } else if (userBirthday.contains(",")) {
        val date = format3.parse(userBirthday)
        userBirthday = format1.format(date)
      }

      if (temperature.contains("℉")) {
        var temp = temperature.substring(0, temperature.size - 1).toDouble
        temp = (temp - 32) / 1.8
        temperature = temp.formatted("%.1f") + "℃"
      }

      if (!fields(6).equals("?")) {
        var rating = fields(6).toDouble
        rating = (rating - min) / (max - min)
        fields(6) = rating.toString
      }

      fields(4) = reviewDate
      fields(5) = temperature

      fields(8) = userBirthday

      fields
    })

    // 保存为data_Filter2
    filteredData2
      .map(_.mkString("|"))
      .coalesce(1).saveAsTextFile("hdfs://hadoop100:9000/user/hadoop/data_Filter2")

    // 4、数据清洗(缺失值填充)
    // 转换成dataframe
    import spark.implicits._
    val df = filteredData2.map(fields => review(fields(0), fields(1).toDouble, fields(2).toDouble, fields(3).toDouble,
      fields(4), fields(5), (if (fields(6).equals("?")) Double.NaN else fields(6).toDouble), fields(7),
      fields(8), fields(9), fields(10), (if (fields(11).equals("?")) Double.NaN else fields(11).toDouble))).toDF()

    df.cache()
    //    println("df:" + df.count())
    //    df.show()

    // 填充user_income
    val testData = df.filter(col("user_income").isNaN)
    //    println("testData:" + testData.count())
    //    testData.show()

    val trainingData = df.filter(!col("user_income").isNaN)
    //    println("trainingData:" + trainingData.count())
    //    trainingData.show()

    val indexer1 = new StringIndexer()
      .setInputCol("user_nationality")
      .setOutputCol("user_nationality_index")

    val indexer2 = new StringIndexer()
      .setInputCol("user_career")
      .setOutputCol("user_career_index")

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("user_nationality_index", "user_career_index"))
      .setOutputCol("features")

    val rf = new RandomForestRegressor()
      .setNumTrees(1)
      .setLabelCol("user_income")
      .setFeaturesCol("features")

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(indexer1, indexer2, assembler1, rf))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)


    val incomeFilled: DataFrame = predictions.select("review_id", "longitude", "latitude", "altitude",
      "review_date", "temperature", "rating_score", "user_id",
      "user_birthday", "user_nationality", "user_career", "prediction")
      .withColumnRenamed("prediction", "user_income")

    val df2 = trainingData.union(incomeFilled)
    println("success")
    df2.show()


    // 填充rating
    val testData2 = df2.filter(col("rating_score").isNaN)
    println("testData2")
    testData2.show()

    val trainingData2 = df2.filter(!col("rating_score").isNaN)
    println("trainingData2")
    trainingData2.show()

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("longitude", "latitude", "altitude", "user_income"))
      .setOutputCol("features")

    val rf2 = new RandomForestRegressor()
      .setLabelCol("rating_score")
      .setFeaturesCol("features")

    // Chain indexer and forest in a Pipeline.
    val pipeline2 = new Pipeline()
      .setStages(Array(assembler2, rf2))

    // Train model. This also runs the indexer.
    val model2 = pipeline2.fit(trainingData2)

    // Make predictions.
    val predictions2 = model2.transform(testData2)

    val ratingFilled = predictions2.select("review_id", "longitude", "latitude", "altitude",
      "review_date", "temperature", "prediction", "user_id",
      "user_birthday", "user_nationality", "user_career", "user_income")
      .withColumnRenamed("prediction", "rating_score")

    println(ratingFilled.count())
    ratingFilled.show()

    //保存为data_Done
    trainingData2.union(ratingFilled).rdd
      .map(_.mkString("|"))
      .coalesce(1).saveAsTextFile("hdfs://hadoop100:9000/user/hadoop/data_Done")

    sc.stop()
  }
}

/*
1. string review_id
2. double longitude
3. double latitude
4. double altitude
5. string review_date
6. string temperature
7. double rating
8. string user_id
9. string user_birthday
10. string user_nationality
11. category user_career
12. double user_income
 */
//每行数据构成的样例类
case class review(review_id: String, longitude: Double, latitude: Double, altitude: Double,
                  review_date: String, temperature: String, rating_score: Double, user_id: String,
                  user_birthday: String, user_nationality: String, user_career: String, user_income: Double)