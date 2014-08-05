/**
 * MovieSimilarities.scala
 * by Linus Yang & Tomas Tauber
 *
 * Originally by Nick Pentreath
 * http://mlnick.github.io/blog/2013/04/01/movie-recommendations-and-more-with-spark/
 * 
 * Spark Live Demo Section
 * for Big Data Analytics in Scala
 * http://www.meetup.com/HK-Functional-programming/
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
 * A port of [[http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/]]
 * to Spark.
 * Uses movie ratings data from MovieLens 100k dataset found at [[http://www.grouplens.org/node/73]]
 */

// Worker class must be serializable for running on Spark cluster
class Worker extends java.io.Serializable {
  // Default movie for comparison
  var movieName = "Star Wars (1977)"
  var movieTopNum = 10

  // Default filenames of movie and rating data
  var movieFilename = "ml-100k/u.item"
  var ratingFilename = "ml-100k/ua.base"

  // Intermediate variables
  var movies: RDD[(Int, String)] = _
  var ratings: RDD[(Int, Int, Int)] = _
  var ratingPairs: RDD[(Int, ((Int, Int, Int, Int), (Int, Int, Int, Int)))] = _
  var similarities: RDD[((Int, Int), Double)] = _

  // Log actions to console
  def log(step: Int, action: String, pattern: String) {
    println("Step %d. %s:\n\t%s".format(step, action, pattern))
  }

  // Read movies and ratings from files
  def read(sc: SparkContext): Worker = {
    // extract (movieid, moviename) from movie data
    movies = sc.textFile(movieFilename)
      .map(line => {
        val fields = line.split("\\|")
        (fields(0).toInt, fields(1))
    })

    // extract (userid, movieid, rating) from ratings data
    ratings = sc.textFile(ratingFilename)
      .map(line => {
        val fields = line.split("\t")
        (fields(0).toInt, fields(1).toInt, fields(2).toInt)
    })
    log(1, "Extract from file", "T1(user, movie, rating)")

    return this
  }

  // Generate rating pairs
  def pair(): Worker = {
    // get num raters per movie, keyed on movie id
    val numRatersPerMovie = ratings
      .groupBy(tup => tup._2)
      .map(grouped => (grouped._1, grouped._2.size))
    log(2, "Count raters by movie", "T2(movie, numRaters)")

    // join ratings with num raters on movie id
    // ratingsWithSize now contains the following fields: (user, movie, rating, numRaters).
    val ratingsWithSize = ratings
      .groupBy(tup => tup._2)
      .join(numRatersPerMovie)
      .flatMap(joined => {
        joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))
    })
    log(3, "Merge ratings with numRaters", "T1 ⋈ T2 -> T3(user, movie, rating, numRaters)")

    // dummy copy of ratings for self join
    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    // join on userid and filter movie pairs such that we don't double-count and exclude self-pairs
    ratingPairs =
      ratingsWithSize
      .keyBy(tup => tup._1)
      .join(ratings2)
      .filter(f => f._2._1._2 < f._2._2._2)
    log(4, "Get rating pairs", "T3 ⋈ T3' -> T4(movie, rating, numRaters, movie', rating', numRaters')")

    return this
  }

  // Calculate similarity
  def calc(): Worker  = {
    /**
     * The correlation between two vectors A, B is
     *   cov(A, B) / (stdDev(A) * stdDev(B))
     *
     * This is equivalent to
     *   [n * dotProduct(A, B) - sum(A) * sum(B)] /
     *     sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
     */
    val correlation = (size : Double, dotProduct : Double, ratingSum : Double,
                    rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) => {

      val numerator = size * dotProduct - ratingSum * rating2Sum
      val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
        scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

      numerator / denominator
    }

    /**
     * Regularize correlation by adding virtual pseudocounts over a prior:
     *   RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
     * where w = # actualPairs / (# actualPairs + # virtualPairs).
     */
    val regularizedCorrelation = (size : Double, dotProduct : Double, ratingSum : Double,
                               rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double,
                               virtualCount : Double, priorCorrelation : Double) => {

      val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
      val w = size / (size + virtualCount)

      w * unregularizedCorrelation + (1 - w) * priorCorrelation
    }

    /**
   * The Jaccard Similarity between two sets A, B is
   *   |Intersection(A, B)| / |Union(A, B)|
   */
    val jaccardSimilarity = (usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) => {
      val union = totalUsers1 + totalUsers2 - usersInCommon
      usersInCommon / union
    }

    /**
     * compute similarity metrics for each movie pair
     *
     * By grouping on (movie, movie'), we can now get 
     * all the people who rated any pair of movies.
     */
    similarities =
      ratingPairs
      .map(data => {
        val key = (data._2._1._2, data._2._2._2)
        val stats =
          (data._2._1._3 * data._2._2._3, // rating 1 * rating 2
            data._2._1._3,                // rating movie 1
            data._2._2._3,                // rating movie 2
            math.pow(data._2._1._3, 2),   // square of rating movie 1
            math.pow(data._2._2._3, 2),   // square of rating movie 2
            data._2._1._4,                // number of raters movie 1
            data._2._2._4)                // number of raters movie 2
        (key, stats)
      })
      .groupByKey()
      .map(data => {
        val key = data._1
        val vals = data._2
        val size = vals.size
        val dotProduct = vals.map(f => f._1).sum
        val ratingSum = vals.map(f => f._2).sum
        val rating2Sum = vals.map(f => f._3).sum
        val ratingNormSq = vals.map(f => f._4).sum
        val rating2NormSq = vals.map(f => f._5).sum
        val numRaters = vals.map(f => f._6).max
        val numRaters2 = vals.map(f => f._7).max
        val corr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, movieTopNum, 0)
        // We don't use jaccard here
        // val jaccard = jaccardSimilarity(size, numRaters, numRaters2)
        (key, corr)
      })
    log(5, "Calculate correlation", "T4 -> T5(movie, corr)")

    return this
  }

  // Print results
  def show() {
    // for local use to map id <-> movie name for pretty-printing
    val movieNames = movies.collectAsMap()

    // test a few movies out (substitute the contains call with the relevant movie name
    val sample = similarities.filter(m => {
      val movietuple = m._1
      (movieNames(movietuple._1).contains(movieName))
    })

    // collect results, excluding NaNs if applicable
    // test for NaNs must use equals rather than ==
    val result = sample.map(v => {
      val m2 = v._1._2
      val corr = v._2
      (movieNames(m2), corr)
    }).collect().filter(e => !(e._2 equals Double.NaN))
    .sortBy(elem => -elem._2).take(movieTopNum)

    // print the N-top results
    println("=== Recommendation for \"%s\" ===".format(movieName))
    println("Confidence | Movie")
    result.foreach(r =>
      println(r._2.formatted("%10.4f") + " | " + r._1)
    )
  }

  // Run all
  def run(sc: SparkContext) {
    read(sc).pair().calc().show()
  }
}

// Standalone app for batch running
object MovieSimilarities {
  def main(args: Array[String]) {
    // Initialize worker object
    val w = new Worker()

    // Parse arguments
    if (args.length > 2) {
      w.movieName = args(0)
      w.movieFilename = args(1)
      w.ratingFilename = args(2)
    } else if (args.length > 0) {
      w.movieName = args(0)
    }

    // Initialize Spark context
    val conf = new SparkConf().setAppName("MovieSimilarities")
    val sc = new SparkContext(conf)

    // Run the worker
    w.run(sc)
  }
}
