/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import scala.collection.mutable
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ ALS, MatrixFactorizationModel, Rating }

// define the schemas using a case classes
// input format MovieID::Title::Genres
case class Movie(movieId: Int, title: String)

// input format UserID::Gender::Age::Occupation::Zip-code
case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

object SparkMovieRec {
  def main(args: Array[String]) {

    // function to parse input into Movie class  
    def parseMovie(str: String): Movie = {
      val fields = str.split("::")
      assert(fields.size == 3)
      Movie(fields(0).toInt, fields(1))
    }

    // function to parse input into User class
    def parseUser(str: String): User = {
      val fields = str.split("::")
      assert(fields.size == 5)
      User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
    }

    // function to parse input UserID::MovieID::Rating
    // and pass into  constructor for org.apache.spark.mllib.recommendation.Rating class
    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }

    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    // load the data into an RDD
    val ratingText = sc.textFile("/user/user01/moviemed/ratings.dat")
    val ratingsRDD = ratingText.map(parseRating).cache()
    // count number of total ratings
    val numRatings = ratingsRDD.count()
    // count number of users who rated a movie 
    val numUsers = ratingsRDD.map(_.user).distinct().count()
    // count number of movies rated 
    val numMovies = ratingsRDD.map(_.product).distinct().count()
    println("Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // load the data into DataFrames
    val usersDF = sc.textFile("/user/user01/moviemed/users.dat").map(parseUser).toDF()
    val moviesDF = sc.textFile("/user/user01/moviemed/movies.dat").map(parseMovie).toDF()
    val ratingsDF = ratingsRDD.toDF()

    ratingsDF.registerTempTable("ratings")
    moviesDF.registerTempTable("movies")
    usersDF.registerTempTable("users")

    // Count the max, min ratings along with the number of users who have rated a movie. Display the title, max rating, min rating, number of users. 
    val results =sqlContext.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu from(SELECT ratings.product, max(ratings.rating) as maxr, min(ratings.rating) as minr,count(distinct user) as cntu FROM ratings group by ratings.product ) movierates join movies on movierates.product=movies.movieId order by movierates.cntu desc ")
    results.take(20).foreach(println)

    // Display the Title for movies with ratings > 4
    val results2 = sqlContext.sql("SELECT ratings.user, ratings.product, ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product where ratings.user=4169 and ratings.rating > 4 order by ratings.rating desc ")
    results2.take(20).foreach(println)

    // Show the top 10 most-active users and how many times they rated a movie
    val mostActiveUsersSchemaRDD = sqlContext.sql("SELECT ratings.user, count(*) as ct from ratings group by ratings.user order by ct desc limit 10")
    mostActiveUsersSchemaRDD.take(20).foreach(println)

    // Randomly split ratings RDD into training data RDD (80%) and test data RDD (20%)
    val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)

    val trainingRatingsRDD = splits(0).cache()
    val testRatingsRDD = splits(1).cache()

    val numTraining = trainingRatingsRDD.count()
    val numTest = testRatingsRDD.count()
    println(s"Training: $numTraining, test: $numTest.")

    // Build the recommendation model using ALS with rank=20, iterations=10
    val model = ALS.train(trainingRatingsRDD, 10, 20)

    val topRecsForUser = model.recommendProducts(4169, 10)
    println(topRecsForUser.mkString("\n"))

    // get movie titles to show with recommendations 
    val movieTitles = moviesDF.map(array => (array(0), array(1))).collectAsMap()

    // print out top recommendations for user 4169 with titles
    topRecsForUser.map(rating => (movieTitles(rating.product), rating.rating)).foreach(println)

    // Test the model by predicting ratings for test data
    // get predicted ratings to compare to test ratings
    val predictionsForTestRDD = model.predict(testRatingsRDD.map { case Rating(user, product, rating) => (user, product) })

    predictionsForTestRDD.take(10).mkString("\n")

    // prepare the predictions for comparison
    val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }
    // prepare the test for comparison
    val testKeyedByUserProductRDD = testRatingsRDD.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }

    //Join the test with the predictions
    val testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)

    testAndPredictionsJoinedRDD.take(10).mkString("\n")

    val falsePositives = testAndPredictionsJoinedRDD.filter { case ((user, product), (ratingT, ratingP)) => (ratingT <= 1 && ratingP >= 4) }

    //Evaluate the model using Mean Absolute Error (MAE) between test and predictions 
    val meanAbsoluteError = testAndPredictionsJoinedRDD.map {
      case ((user, product), (testRating, predRating)) =>
        val err = (testRating - predRating)
        Math.abs(err)
    }.mean()

  }
}
