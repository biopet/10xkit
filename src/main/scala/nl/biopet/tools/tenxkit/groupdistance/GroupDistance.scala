package nl.biopet.tools.tenxkit.groupdistance

import java.io.{File, PrintWriter}

import nl.biopet.tools.tenxkit
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import nl.biopet.tools.tenxkit.{DistanceMatrix, VariantCall}
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.clustering.KMeans
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
    val variants = if (cmdArgs.inputFile.isDirectory) {
      VariantCall
        .fromPartitionedVcf(cmdArgs.inputFile,
                            cmdArgs.reference,
                            correctCellsMap)
    } else {
      VariantCall
        .fromVcfFile(cmdArgs.inputFile,
                     cmdArgs.reference,
                     correctCellsMap,
                     50000000)
    }

    val vectors = variants
      .flatMap { v =>
        val alleles = 0 :: v.altAlleles.indices.map(_ + 1).toList
        correctCells.value.indices.map { sample =>
          val sa = v.samples.get(sample) match {
            case Some(a) => alleles.map(a(_).total.toDouble)
            case _       => alleles.map(_ => 0.0)
          }
          sample -> sa
        }
      }
      .groupByKey //(correctCells.value.size)
      .map { x =>
        (x._1, Vectors.dense(x._2.flatten.toArray))
      }.cache()
//    val df = vectors.toDF("sample", "features").cache()
//    val bla1 = vectors.count()
//    val bla2 = df.count()
//    val bla3 = variants.count()

    // Cluster the data into two classes using KMeans
    val numClusters = 5
    val numIterations = 20
    val clusters = KMeans.train(vectors.map(_._2), numClusters, numIterations)

    val c = vectors.groupBy(x => clusters.predict(x._2)).map(x => x._1 -> x._2.map(s => correctCells.value(s._1))).collectAsMap()

//    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
//    println(s"Pearson correlation matrix:\n $coeff1")
//
//    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
//    println(s"Spearman correlation matrix:\n $coeff2")

    c.foreach { case (idx, samples) =>
      val writer = new PrintWriter(new File(cmdArgs.outputDir, s"cluster.$idx.txt"))
      samples.foreach(writer.println)
      writer.close()
    }

    logger.info("Done")
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
