package de.uhh.lt.jst.dt

import de.uhh.lt.jst.utils.Config
import org.apache.spark.{SparkConf, SparkContext}

object WordSimFromTermContext {

  val wordsPerFeatureNumDefault = 1000
  val significanceMinDefault = 0.0
  val wordFeatureMinCountDefault = 2
  val wordMinCountDefault = 2
  val featureMinCountDefault = 2
  val significanceTypeDefault = "LMI"
  val featuresPerWordNumDefault = 1000
  val similarWordsMaxNumDefault = 200

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: word-counts feature-counts word-feature-counts output-dir [parameters]")
      println("parameters: wordsPerFeatureNum featuresPerWordNum wordMinCount featureMinCount wordFeatureMinCount significanceMin significanceType similarWordsMaxNum")
      return
    }

    val config = new Config {
      val wordCountsPath = args(0)
      val featureCountsPath = args(1)
      val wordFeatureCountsPath = args(2)
      val outputDir = args(3)
      val wordsPerFeatureNum: Int = if (args.length > 4) args(4).toInt else wordsPerFeatureNumDefault
      val featuresPerWordNum: Int = if (args.length > 5) args(5).toInt else featuresPerWordNumDefault
      val wordMinCount: Int = if (args.length > 6) args(6).toInt else wordMinCountDefault
      val featureMinCount: Int = if (args.length > 7) args(7).toInt else featureMinCountDefault
      val wordFeatureMinCount: Int = if (args.length > 8) args(8).toInt else wordFeatureMinCountDefault
      val significanceMin: Double = if (args.length > 9) args(9).toDouble else significanceMinDefault
      val significanceType: String = if (args.length > 10) args(10) else significanceTypeDefault
      val similarWordsMaxNum: Int = if (args.length > 11) args(11).toInt else similarWordsMaxNumDefault
    }

    val conf = new SparkConf().setAppName("JST: WordSimFromCounts")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, config)
    //wordCountsPath, featureCountsPath, wordFeatureCountsPath, outputDir, wordsPerFeatureNum, featuresPerWordNum, wordMinCount, featureMinCount, wordFeatureMinCount, significanceMin, significanceType, similarWordsMaxNum)
  }

  def run(sc: SparkContext, config: Config {
    val significanceType: String
    val featuresPerWordNum: Int
    val featureMinCount: Int
    val wordsPerFeatureNum: Int
    val similarWordsMaxNum: Int
    val wordMinCount: Int
    val featureCountsPath: String
    val wordFeatureMinCount: Int
    val outputDir: String
    val wordCountsPath: String
    val wordFeatureCountsPath: String
    val significanceMin: Double
  }): Unit = {

    val wordFeatureCounts = sc.textFile(config.wordFeatureCountsPath)
      .map(line => line.split("\t"))
      .map {
        case Array(word, feature, count) => (word, (feature, count.toInt))
        case _ => ("?", ("?", 0))
      }

    val wordCounts = sc.textFile(config.wordCountsPath)
      .map(line => line.split("\t"))
      .map {
        case Array(word, count) => (word, count.toInt)
        case _ => ("?", 0)
      }

    val featureCounts = sc.textFile(config.featureCountsPath)
      .map(line => line.split("\t"))
      .map {
        case Array(feature, count) => (feature, count.toInt)
        case _ => ("?", 0)
      }


    val (simsPath, simsWithFeaturesPath, featuresPath) = WordSimLib.computeWordSimsWithFeatures(
      wordFeatureCounts,
      wordCounts,
      featureCounts,
      config = new Config {
        val outputDir: String = config.outputDir
        val wordsPerFeatureMax: Int = config.wordsPerFeatureNum
        val featuresPerWordMaxp: Int = config.featuresPerWordNum
        val wordCountMin: Int = config.wordMinCount
        val featureCountMin: Int = config.featureMinCount
        val wordFeatureCountMin: Int = config.wordFeatureMinCount
        val significanceMin: Double = config.significanceMin
        val similarWordsMaxNum: Int = config.similarWordsMaxNum
        val significanceType: String = config.significanceType
      })

    println(s"Word similarities: $simsPath")
    println(s"Word similarities with features: $simsWithFeaturesPath")
    println(s"Features: $featuresPath")
  }
}
