

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import utils.Const
import wsd.Clusters2Features

class Clusters2FeaturesTest extends FlatSpec with Matchers {
    "The Cluster2Features object" should "generate result" in {
        val sensesPath =  getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
        val outputPath =  sensesPath + "-output"

        val conf = new SparkConf()
            .setAppName("JST: Clusters2Features")
            .setMaster("local[4]")
        val sc = new SparkContext(conf)
        Clusters2Features.run(sensesPath, outputPath, sc)
    }
}




