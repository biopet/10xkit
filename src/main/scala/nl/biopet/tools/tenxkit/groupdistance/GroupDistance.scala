package nl.biopet.tools.tenxkit.groupdistance

import java.io.{File, PrintWriter}

import nl.biopet.tools.tenxkit
import org.apache.spark.ml.clustering._
import nl.biopet.tools.tenxkit.{DistanceMatrix, VariantCall}
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object GroupDistance extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster)
    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    logger.info("Reading input data")
    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val vectors = (if (cmdArgs.inputFile.isDirectory) {
                     variantsToVectors(VariantCall
                                         .fromPartitionedVcf(cmdArgs.inputFile,
                                                             cmdArgs.reference,
                                                             correctCellsMap),
                                       correctCells)
                   } else if (cmdArgs.inputFile.getName.endsWith(".vcf.gz")) {
                     variantsToVectors(VariantCall
                                         .fromVcfFile(cmdArgs.inputFile,
                                                      cmdArgs.reference,
                                                      correctCellsMap,
                                                      50000000),
                                       correctCells)
                   } else {
                     distanceMatrixToVectors(cmdArgs.inputFile, correctCells)
                   }).toDF("sample", "features").cache()

//    val bla1 = vectors.count()
//    val bla2 = df.count()
//    val bla3 = variants.count()

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(2)

    // Cluster the data into two classes using KMeans
    //val model = KMeans.train(vectors.map(_._2), cmdArgs.numClusters, cmdArgs.numIterations, "k-means||", cmdArgs.seed)
    val kmeans = new KMeans()
      .setK(cmdArgs.numClusters)
      .setMaxIter(cmdArgs.numIterations)
      .setSeed(cmdArgs.seed)

    val bkm = new BisectingKMeans()
      .setK(cmdArgs.numClusters)
      //.setMaxIter(cmdArgs.numIterations)
      .setSeed(cmdArgs.seed)

    val lda = new LDA()
      .setK(cmdArgs.numClusters)
      //.setMaxIter(cmdArgs.numIterations)
      .setSeed(cmdArgs.seed)

    val gmm = new GaussianMixture()
      .setK(cmdArgs.numClusters)
      //.setMaxIter(cmdArgs.numIterations)
      .setSeed(cmdArgs.seed)

    val lr = new LogisticRegression()
      .setMaxIter(10)

    val pipeline = new Pipeline()
      .setStages(Array(pca, bkm))

    val model = lr.fit(vectors)

    val predictions = model
      .transform(vectors)
      .select("sample", "prediction")
      .as[Prediction]
      .collect()
      .groupBy(_.prediction)
      .map(x => x._1 -> x._2.map(s => s.sample))

    predictions.foreach {
      case (idx, samples) =>
        val writer =
          new PrintWriter(new File(cmdArgs.outputDir, s"cluster.$idx.txt"))
        samples.foreach(s => writer.println(correctCells.value(s)))
        writer.close()
    }

//    val cluster1 = sc.parallelize(predictions(1)).toDF("sample2")
//    val df2 = cluster1.join(vectors, cluster1("sample2") === vectors("sample"))
//    val model2 = kmeans.setK(2).fit(df2)
//
//    val predictions2 = model2.transform(df2)
//      .select("sample", "prediction").as[Prediction].collect()
//      .groupBy(_.prediction)
//      .map(x => x._1 -> x._2.map(s => s.sample))

    logger.info("Done")
  }

  case class Prediction(sample: Int, prediction: Int)

  def variantsToVectors(
      variants: RDD[VariantCall],
      correctCells: Broadcast[Array[String]]): RDD[(Int, linalg.Vector)] = {
    variants
      .flatMap { v =>
        val alleles = 0 :: v.altAlleles.indices.map(_ + 1).toList
        correctCells.value.indices.map { sample =>
          val sa = v.samples.get(sample) match {
            case Some(a) =>
              val total = a.map(_.total).sum
              alleles.map(a(_).total.toDouble / total)
            case _ => alleles.map(_ => 0.0)
          }
          sample -> (v.contig, v.pos, sa)
        }
      }
      .groupByKey(correctCells.value.length)
      .map {
        case (sample, list) =>
          val sorted = list.toList.sortBy(y => (y._1, y._2))
          (sample, Vectors.dense(sorted.flatMap(_._3).toArray))
      }
  }

  def distanceMatrixToVectors(inputFile: File,
                              correctSamples: Broadcast[Array[String]])(
      implicit sc: SparkContext): RDD[(Int, linalg.Vector)] = {
    val matrix = DistanceMatrix.fromFile(inputFile)
    require(matrix.samples sameElements correctSamples.value)
    val samples = matrix.samples.indices.toList
    val samplesFiltered = samples.filter(s1 =>
      samples.map(s2 => matrix(s1, s2)).count(_.isDefined) >= 1000)
    logger.info(s"Removed ${samples.size - samplesFiltered.size} samples")
    val vectors = samplesFiltered.map(
      s1 =>
        s1 -> Vectors.dense(
          samplesFiltered.map(s2 => matrix(s1, s2).getOrElse(0.0)).toArray))
    sc.parallelize(vectors)//, vectors.size)
  }

  def descriptionText: String =
    """
      |
    """.stripMargin

  def manualText: String =
    """
      |
    """.stripMargin

  def exampleText: String =
    """
      |
    """.stripMargin

}
