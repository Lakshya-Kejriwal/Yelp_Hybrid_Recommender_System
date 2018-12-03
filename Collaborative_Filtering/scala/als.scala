
import java.util
import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD


object als {


  def main(args: Array[String]): Unit = {

    def row(line: Array[String]): ((String, String), Int) = {
      ((line(0), line(1)), line(2).toInt)
    }

    val start_time = System.nanoTime()

    val conf = new SparkConf().setAppName("ModelBasedCF").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val train_file = args(0)
    val test_file = args(1)
    val city = args(2)
    var mode = 0
    if(args.length == 4)
      {
        mode = args(3).toInt
      }

    val train = sc.textFile(train_file)
    val test = sc.textFile(test_file)

    println("test_data_count = "+ test.count())
    println("train_data_count = "+ train.count())

    // create rdd of the train data and remove the header
    val train_data = train
      .map(item => item.split(","))

    // create rdd of the test data and remove the header
    val test_data = test.map(item=> item.split(","))


    val trainRDD = train_data.map(row)

    val testRDD = test_data.map(row)

    // Remove test data from train if any
    val trainData = trainRDD.subtractByKey(testRDD)

    /* Combine all train an test data
       To convert user and prod to unique number id for training on ALS
     */
    val data = trainData.union(testRDD)

    val names = data.map(r=>(r._1._1)).distinct.sortBy(x => x).zipWithIndex.collectAsMap
    val products = data.map(r=>(r._1._2)).distinct.sortBy(x => x).zipWithIndex.collectAsMap


    var partitioner = new HashPartitioner(100)


    // Convert RDD to scala type Rating
    val ratings = trainData.map(r => Rating(names(r._1._1).toInt, products(r._1._2).toInt, r._2.toDouble))

    val test_ratings = testRDD.map(r => Rating(names(r._1._1).toInt, products(r._1._2).toInt, r._2))

    val test_f = test_ratings.map(r => ((r.user, r.product), 0))

    // Compute average for user and products
    val avg_rating = ratings.map(r => (r.user, r.rating)).groupByKey().mapValues(_.toList).map(r => (r._1, r._2.sum / r._2.length))//.partitionBy(partitioner)
    val prod_rating = ratings.map(r => (r.product, r.rating)).groupByKey().mapValues(_.toList).map(r => (r._1, r._2.sum / r._2.length))//.partitionBy(partitioner)

    var map = trainData.map(r => ((names(r._1._1).toInt, products(r._1._2).toInt), r._2.toDouble))


    var missing_val = map.subtractByKey(test_f)

    /* Normalize rating values by subtracting user rating from average product rating
     */

    val user = missing_val.map(r => (r._1._1, (r._1._2, r._2))).join(avg_rating).map(r => ((r._1, r._2._1._1), r._2._2))

    map = map.union(user)


    val prod = missing_val.map(r => (r._1._2, (r._1._1, r._2))).join(prod_rating).map(r => ((r._2._1._1, r._1), r._2._2))

    map = map.union(prod)


    val tojoin = map.map(r => (r._1._2, (r._1._1, r._2)))

    val bias = tojoin.join(prod_rating).map(r => Rating(r._2._1._1, r._1, r._2._1._2-r._2._2))

    // Train ALS model on normalized rating values
    val rank = 12
    val numIterations = 20
    val model = ALS.train(bias, rank, numIterations, 0.1)


    partitioner = new HashPartitioner(1000)

    // Evaluate the model on rating data

    val usersProducts = test_ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val trainProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }


    //Make predictions on test data
    val temp_predictions =
      model.predict(usersProducts).map{case Rating(x, y, z) => (y, (x, z))}.join(prod_rating).map(r => Rating(r._2._1._1, r._1, r._2._2+r._2._1._2)).
        map{ case Rating(x, y, z) => if (z<=0) ((x, y), 0.0) else if (z>=5) ((x, y), 5.0) else ((x, y), z)}

    //Make predictions on train data
    val train_predictions =
      model.predict(trainProducts).map{case Rating(x, y, z) => (y, (x, z))}.join(prod_rating).map(r => Rating(r._2._1._1, r._1, r._2._2+r._2._1._2)).
        map{ case Rating(x, y, z) => if (z<=0) ((x, y), 0.0) else if (z>=5) ((x, y), 5.0) else ((x, y), z)}



    val temp_test = testRDD.map(r => ((names(r._1._1).toInt, products(r._1._2).toInt), r._2.toDouble))

    // Find missing users and products and take their average
    val missing_predictions = temp_test.subtractByKey(temp_predictions).map(r => (r._1._1, r._1._2)).join(avg_rating).map(r => ((r._1, r._2._1), r._2._2))
    val prod_predictions = temp_test.subtractByKey(temp_predictions).map(r => (r._1._2, r._1._1)).join(prod_rating).map(r => ((r._2._1, r._1), r._2._2))


    // Combine all predictions
    var predictions = temp_predictions.union(missing_predictions).union(prod_predictions)


    // Save test predictions file
    val test_output_file = city+"_ALS_test_predictions.txt"

    val bw = new BufferedWriter(new FileWriter(test_output_file))

    val reversenames = for ((k, v) <- names) yield (v, k)
    val reverseprods = for ((k, v) <- products) yield (v, k)

    val x = predictions.collect()
    for (row <- x) {
      bw.write(reversenames(row._1._1) + "," + reverseprods(row._1._2) + "," + row._2 + "\n")
    }

    bw.close()

    // Save train predictions file
    if(mode == 1) {
      val train_output_file = city+"_ALS_train_predictions.txt"
      val bw = new BufferedWriter(new FileWriter(train_output_file))

      val reversenames = for ((k, v) <- names) yield (v, k)
      val reverseprods = for ((k, v) <- products) yield (v, k)

      val x = train_predictions.collect()
      for (row <- x) {
        bw.write(reversenames(row._1._1) + "," + reverseprods(row._1._2) + "," + row._2 + "\n")
      }

      bw.close()
    }


    //Join train and test predictions to compute the RMSE
    val ratesAndPreds = test_ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val train_ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(train_predictions)


    // Compute Test RMSE
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    // Compute Train RMSE
    val train_MSE = train_ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    print("RMSE: " + Math.sqrt(MSE) + "\n")
    print("Train RMSE: " + Math.sqrt(train_MSE) + "\n")
    val end_time = System.nanoTime()
    print("Time: " + ","+((end_time - start_time) / 1000000000) + " sec.")

  }

}


