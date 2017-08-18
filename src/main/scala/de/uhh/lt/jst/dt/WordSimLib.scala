package de.uhh.lt.jst.dt

import de.uhh.lt.jst.utils.{Config, IndexModuloPartitioner}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD

object WordSimLib {
  val DEBUG = false

  def log2(n: Double): Double = {
    math.log(n) / math.log(2)
  }

  /**
    * Computes a log-likelihood ratio approximation
    *
    * @param n    Total number of observations
    * @param n_A  Number of times A was observed
    * @param n_B  Number of times B was observed
    * @param n_AB Number of times A and B were observed together
    */
  def ll(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    val wcL = log2(n_A)
    val fcL = log2(n_B)
    val bcL = log2(n_AB)
    val epsilon = 0.000001
    val res = 2 * (n * log2(n)
      - n_A * wcL
      - n_B * fcL
      + n_AB * bcL
      + (n - n_A - n_B + n_AB) * log2(n - n_A - n_B + n_AB + epsilon)
      + (n_A - n_AB) * log2(n_A - n_AB + epsilon)
      + (n_B - n_AB) * log2(n_B - n_AB + epsilon)
      - (n - n_A) * log2(n - n_A + epsilon)
      - (n - n_B) * log2(n - n_B + epsilon))
    if ((n * n_AB) < (n_A * n_B)) -res.toDouble else res.toDouble
  }

  def descriptivity(n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB.toDouble * n_AB.toDouble / (n_A.toDouble * n_B.toDouble)
  }

  /**
    * Computes the lexicographer's mutual information (LMI) score:<br/>
    *
    * <pre>LMI(A,B) = n_AB * log2( (n*n_AB) / (n_A*n_B) )</pre>
    * <br/>
    * Reference:
    * Kilgarri, A., Rychly, P., Smrz, P., Tugwell, D.: The sketch engine. In: <i>Proceedings of Euralex</i>, Lorient, France (2004) 105-116
    *
    * @param n    Total number of observations
    * @param n_A  Number of times A was observed
    * @param n_B  Number of times B was observed
    * @param n_AB Number of times A and B were observed together
    */
  def lmi(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB * (log2(n * n_AB) - log2(n_A * n_B))
  }

  def cov(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB.toDouble / n_A.toDouble
  }

  def freq(n: Long, n_A: Long, n_B: Long, n_AB: Long): Double = {
    n_AB.toDouble
  }

  def computeWordFeatureCounts(file: RDD[String],
                               outDir: String)
  : (RDD[(String, (String, Int))], RDD[(String, Int)], RDD[(String, Int)]) = {
    val wordFeaturesOccurrences = file
      .map(line => line.split("\t"))
      .map {
        case Array(word, feature, dataset, wordPos, featurePos) => (word, feature, dataset.hashCode, wordPos, featurePos)
        case _ => ("BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE", "BROKEN_LINE")
      }

    val wordFeatureCounts = wordFeaturesOccurrences
      .map({ case (word, feature, dataset, wordPos, featurePos) => ((word, feature, dataset, wordPos, featurePos), 1) })
      .reduceByKey((v1, v2) => v1 + v2) // count same occurences only once (make them unique)
      .map({ case ((word, feature, dataset, wordPos, featurePos), numOccurrences) => ((word, feature), 1) })
      .reduceByKey((v1, v2) => v1 + v2)
      .map({ case ((word, feature), count) => (word, (feature, count)) })
    wordFeatureCounts.cache()

    val wordCounts = wordFeaturesOccurrences
      .map({ case (word, feature, dataset, wordPos, featurePos) => ((word, dataset, wordPos), 1) })
      .reduceByKey((v1, v2) => v1 + v2)
      .map({ case ((word, dataset, wordPos), numOccurrences) => (word, 1) })
      .reduceByKey((v1, v2) => v1 + v2)
    wordCounts.cache()

    val featureCounts = wordFeaturesOccurrences
      .map({ case (word, feature, dataset, wordPos, featurePos) => ((feature, dataset, featurePos), 1) })
      .reduceByKey((v1, v2) => v1 + v2)
      .map({ case ((feature, dataset, featurePos), numOccurrences) => (feature, 1) })
      .reduceByKey((v1, v2) => v1 + v2)
    featureCounts.cache()

    if (DEBUG) {
      wordCounts
        .map({ case (word, count) => word + "\t" + count })
        .saveAsTextFile(outDir + "/WordCount")
      featureCounts
        .map({ case (feature, count) => feature + "\t" + count })
        .saveAsTextFile(outDir + "/FeatureCount")
      wordFeatureCounts
        .map({ case (word, (feature, count)) => word + "\t" + feature + "\t" + count })
        .saveAsTextFile(outDir + "/WordFeatureCount")
    }

    (wordFeatureCounts, wordCounts, featureCounts)
  }

  def computeFeatureScores(
    wordFeatureCounts: RDD[(String, (String, Int))],
    wordCounts: RDD[(String, Int)],
    featureCounts: RDD[(String, Int)],
    config: Config {
      val outputDir: String
      val wordsPerFeatureNum: Int
      val featuresPerWordNum: Int
      val wordCountMin: Int
      val featureCountMin: Int
      val wordFeatureCountMin: Int
      val significanceMin: Double
      val significance: (Long, Long, Long, Long) => Double
  }): RDD[(String, Array[(String, Double)])] = {

    val wordFeatureCountsFiltered =
      if (config.wordFeatureCountMin > 1) wordFeatureCounts.filter({ case (word, (feature, wfc)) => wfc >= config.wordFeatureCountMin })
      else wordFeatureCounts

    var featureCountsFiltered =
      if (config.featureCountMin > 1) featureCounts.filter({ case (feature, fc) => fc >= config.featureCountMin })
      else featureCounts

    val wordsPerFeatureCounts = wordFeatureCountsFiltered
      .map { case (word, (feature, wfc)) => (feature, word) }
      .groupByKey()
      .mapValues(v => v.size)
      .filter { case (feature, numWords) => numWords <= config.wordsPerFeatureNum }

    featureCountsFiltered = featureCountsFiltered
      .join(wordsPerFeatureCounts) // filter using a join
      .map { case (feature, (fc, fwc)) => (feature, fc) } // remove unnecessary data from join
    featureCountsFiltered.cache()

    val wordCountsFiltered =
      if (config.wordCountMin > 1) wordCounts.filter({ case (word, wc) => wc >= config.wordCountMin })
      else wordCounts
    wordCountsFiltered.cache()

    // Since word counts and feature counts are based on unfiltered word-feature
    // occurrences, n must be based on unfiltered word-feature counts as well
    val n = wordFeatureCounts
      .map({ case (word, (feature, wfc)) => (feature, (word, wfc)) })
      .aggregate(0L)(_ + _._2._2.toLong, _ + _) // we need Long because n might exceed the max. Int value

    val featuresPerWordWithScore = wordFeatureCountsFiltered
      .join(wordCountsFiltered)
      .map({ case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc)) })
      .join(featureCountsFiltered)
      .map({ case (feature, ((word, wfc, wc), fc)) => (word, (feature, config.significance(n, wc, fc, wfc))) })
      .filter({ case (word, (feature, score)) => score >= config.significanceMin })
      .groupByKey()
      // (word, [(feature, score), (feature, score), ...])
      .mapValues(featureScores => featureScores.toArray.sortWith({ case ((_, s1), (_, s2)) => s1 > s2 }).take(config.featuresPerWordNum)) // sort by value desc

    if (DEBUG) {
      wordFeatureCountsFiltered
        .join(wordCountsFiltered)
        .map({ case (word, ((feature, wfc), wc)) => (feature, (word, wfc, wc)) })
        .join(featureCountsFiltered)
        .map({ case (feature, ((word, wfc, wc), fc)) => (word, feature, wc, fc, wfc, config.significance(n, wc, fc, wfc)) })
        .sortBy({ case (word, feature, wc, fc, wfc, score) => score }, ascending = false)
        .map({ case (word, feature, wc, fc, wfc, score) => word + "\t" + feature + "\t" + wc + "\t" + fc + "\t" + wfc + "\t" + n + "\t" + score })
        .saveAsTextFile(config.outputDir + "/AllValuesPerWord")
    }

    featuresPerWordWithScore
  }

  def getSignificance(significanceType: String): (Long, Long, Long, Long) => Double = {
    significanceType match {
      case "LMI" => WordSimLib.lmi _
      case "COV" => WordSimLib.cov _
      case "FREQ" => WordSimLib.freq _
      case _ => WordSimLib.ll _
    }
  }

  def computeWordSimsWithFeatures(
    wordFeatureCounts: RDD[(String, (String, Int))],
    wordCounts: RDD[(String, Int)],
    featureCounts: RDD[(String, Int)],
    config: Config {
      val outputDir: String
      val wordsPerFeatureMax: Int
      val featuresPerWordMaxp: Int
      val wordCountMin: Int
      val featureCountMin: Int
      val wordFeatureCountMin: Int
      val significanceMin: Double
      val similarWordsMaxNum: Int
      val significanceType: String
    })
  : (String, String, String) = {

    val wordSimsPath = config.outputDir + "/SimPruned"
    val wordSimsPrunedWithFeaturesPath = if (DEBUG) config.outputDir + "/SimPrunedWithFeatures" else ""
    val featuresPath = config.outputDir + "/FeaturesPruned"

    // Normalize and prune word features
    val sig = getSignificance(config.significanceType)
    val featuresPerWordWithScore = computeFeatureScores(
      wordFeatureCounts,
      wordCounts,
      featureCounts,
      config = new Config {
        val outputDir: String = config.outputDir
        val wordsPerFeatureNum: Int = config.wordsPerFeatureMax
        val featuresPerWordNum: Int = config.featuresPerWordMaxp
        val wordCountMin: Int = config.wordCountMin
        val featureCountMin: Int = config.featureCountMin
        val wordFeatureCountMin: Int = config.wordFeatureCountMin
        val significanceMin: Double = config.significanceMin
        val significance: (Long, Long, Long, Long) => Double = sig
      })

    featuresPerWordWithScore.cache()

    featuresPerWordWithScore
      .flatMap { case (word, featureScores) => featureScores
        .map { case (feature, score) => f"$word\t$feature\t$score%.5f" }
      }
      .saveAsTextFile(featuresPath, classOf[GzipCodec])

    // Compute word similarities
    val featuresPerWord: RDD[(String, Array[String])] = featuresPerWordWithScore
      .map { case (word, featureScores) => (word, featureScores
        .map { case (feature, score) => feature })
      }

    val wordsPerFeature = featuresPerWord
      .flatMap({ case (word, features) => for (feature <- features.iterator) yield (feature, word) })
      .groupByKey()
      .filter({ case (feature, words) => words.size <= config.wordsPerFeatureMax })
      .sortBy(_._2.size, ascending = false)

    val wordsPerFeatureFairPartitioned = wordsPerFeature
      // the following 4 lines partition the RDD for equal words-per-feature distribution over the partitions
      .zipWithIndex()
      .map { case ((feature, words), index) => (index, (feature, words)) }
      .partitionBy(new de.uhh.lt.jst.utils.IndexModuloPartitioner(1000))
      .map { case (index, (feature, words)) => (feature, words) }
    wordsPerFeatureFairPartitioned.cache()

    val wordSimsAll: RDD[(String, (String, Double))] = wordsPerFeatureFairPartitioned
      .flatMap { case (feature, words) => for (word1 <- words.iterator; word2 <- words.iterator) yield ((word1, word2), 1.0) }
      .reduceByKey { case (score1, score2) => score1 + score2 }
      .map { case ((word1, word2), scoreSum) => (word1, (word2, (scoreSum / config.featuresPerWordMaxp).toDouble)) }
      .sortBy({ case (word, (simWord, score)) => (word, score) }, ascending = false)

    val wordSimsPruned: RDD[(String, (String, Double))] = wordSimsAll
      .groupByKey()
      .mapValues(simWords => simWords.toArray
        .sortWith { case ((w1, s1), (w2, s2)) => s1 > s2 }
        .take(config.similarWordsMaxNum))
      .flatMap { case (word, simWords) => for (simWord <- simWords.iterator) yield (word, simWord) }
      .cache()

    wordSimsPruned
      .map { case (word1, (word2, score)) => f"$word1\t$word2\t$score%.5f" }
      .saveAsTextFile(wordSimsPath, classOf[GzipCodec])

    if (DEBUG) {
      wordsPerFeature
        .map { case (feature, words) => s"$feature\t${words.size}\t${words.mkString("  ")}" }
        .saveAsTextFile(config.outputDir + "/WordsPerFeature", classOf[GzipCodec])

      wordSimsPruned
        .join(featuresPerWord)
        .map({ case (word, ((simWord, score), featureList1)) => (simWord, (word, score, featureList1)) })
        .join(featuresPerWord)
        .map({ case (simWord, ((word, score, featureList1), featureList2)) => (word, (simWord, score, featureList1.toSet.intersect(featureList2.toSet))) })
        .sortBy({ case (word, (simWord, score, mutualFeatureSet)) => (word, score) }, ascending = false)
        .map { case (word1, (word2, score, featureSet)) => f"$word1\t$word2\t$score%.5f\t${featureSet.toList.sorted.mkString("  ")}" }
        .saveAsTextFile(wordSimsPrunedWithFeaturesPath, classOf[GzipCodec])
    }

    (wordSimsPath, wordSimsPrunedWithFeaturesPath, featuresPath)
  }

}
