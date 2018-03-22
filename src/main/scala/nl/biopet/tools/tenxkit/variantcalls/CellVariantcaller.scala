package nl.biopet.tools.tenxkit.variantcalls

import java.io.{File, PrintWriter}

import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import nl.biopet.utils.io
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext._

object CellVariantcaller extends ToolCommand[Args] {
  def argsParser: AbstractOptParser[Args] = new ArgsParser(this)
  def emptyArgs: Args = Args()

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

    val correctCells =
      sc.broadcast(io.getLinesFromFile(cmdArgs.correctCells).toSet)

    val allReads: AlignmentRecordRDD =
      sc.loadBam(cmdArgs.inputFile.getAbsolutePath)

    val filteredReads = allReads.rdd
      .filter(r => !r.getDuplicateRead && r.getReadMapped)
      .flatMap { read =>
        read.getAttributes
          .split("\t")
          .find(_.startsWith(cmdArgs.sampleTag + ":"))
          .map(_.split(":")(2))
          .filter(correctCells.value.contains)
          .map(
            SampleRead(_,
                       read.getContigName,
                       read.getStart,
                       read.getEnd,
                       read.getSequence,
                       read.getQual,
                       read.getCigar,
                       !read.getReadNegativeStrand))
      }
      .flatMap(_.sampleBases)
      .toDS()
      //.cache()

    val writer = new PrintWriter(new File(cmdArgs.outputDir, "counts.tsv"))
    filteredReads
      .groupBy("contig", "pos")
      .count()
      .collect()
      .foreach(row => writer.println(row.mkString("\t")))
    writer.close()

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
