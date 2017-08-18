package de.uhh.lt.jst.dt

import de.uhh.lt.jst.utils._
import org.apache.spark.{SparkConf, SparkContext}


object WordSimFromCounts {
  val default = new  Config {
    val wordsPerFeatureNum: Int= 1000
    val significanceMin: Double = 0.0
    val wordFeatureMinCount: Int = 2
    val wordMinCount: Int = 2
    val featureMinCount: Int = 2
    val significanceType: String = "LMI"
    val featuresPerWordNum: Int = 1000
    val similarWordsMaxNum: Int = 200
  }

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
      val wordsPerFeatureNum: Int = args.getAsIntOrElse(4, default.wordsPerFeatureNum)
      val featuresPerWordNum: Int = args.getAsIntOrElse(5, default.featuresPerWordNum)
      val wordMinCount: Int = args.getAsIntOrElse(6, default.wordMinCount)
      val featureMinCount: Int = args.getAsIntOrElse(7, default.featureMinCount)
      val wordFeatureMinCount: Int = args.getAsIntOrElse(8, default.wordFeatureMinCount)
      val significanceMin: Double = args.getAsDoubleOrElse(9, default.significanceMin)
      val significanceType: String = args.getOrElse(10, default.significanceType)
      val similarWordsMaxNum: Int = args.getAsIntOrElse(11, default.similarWordsMaxNum)
    }

    val conf = new SparkConf().setAppName("JST: WordSimFromCounts")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    run(sc, config)
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

