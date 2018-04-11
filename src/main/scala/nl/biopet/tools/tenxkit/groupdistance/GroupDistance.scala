package nl.biopet.tools.tenxkit.groupdistance

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller.logger
import nl.biopet.tools.tenxkit.{DistanceMatrix, VariantCall}
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
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
        .fromPartitionedVcf(cmdArgs.inputFile, cmdArgs.reference, correctCellsMap)
    } else {
      VariantCall
        .fromVcfFile(cmdArgs.inputFile, cmdArgs.reference, correctCellsMap, 50000000)
    }

    val df = variants.flatMap { v =>
      val alleles = 0 :: v.altAlleles.indices.map(_ + 1).toList
      correctCells.value.indices.map { sample =>
        val sa = v.samples.get(sample) match {
          case Some(a) => alleles.map(a(_).total.toDouble)
          case _ => alleles.map(_ => 0.0)
        }
        sample -> sa
    }
  }.groupByKey//(correctCells.value.size)
      .map{x =>
      (x._1, Vectors.dense(x._2.flatten.toArray))
    }
      .toDF("sample", "features").cache()
    val bla2 = df.head()

//    val data = Seq(
//      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
//      Vectors.dense(4.0, 5.0, 0.0, 3.0),
//      Vectors.dense(6.0, 7.0, 0.0, 8.0),
//      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
//    )

    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")

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
