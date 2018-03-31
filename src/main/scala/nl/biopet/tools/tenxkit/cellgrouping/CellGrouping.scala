package nl.biopet.tools.tenxkit.cellgrouping

import java.io.File

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.variantcalls
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller.logger
import nl.biopet.tools.tenxkit.variantcalls.{CellVariantcaller, VariantCall}
import nl.biopet.utils.ngs.{bam, fasta, vcf}
import nl.biopet.utils.ngs.intervals.BedRecordList
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object CellGrouping extends ToolCommand[Args] {
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

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)

    val futures: ListBuffer[Future[Any]] = ListBuffer()

    val variants: Dataset[VariantCall] = (cmdArgs.inputFile.getName match {
      case name if name.endsWith(".bam") =>
        val result = readBamFile(cmdArgs, correctCells, correctCellsMap)
        futures += result.totalFuture
        Await.result(result.filteredVariants, Duration.Inf)
      case name if name.endsWith(".vcf") || name.endsWith(".vcf.gz") =>
        readVcfFile(cmdArgs, correctCellsMap)
      case _ =>
        throw new IllegalArgumentException(
          "Input file must be a bam or vcf file")
    }).toDS()

    println(variants.count())

    //TODO: Read vcf file
    //TODO: Grouping

    Await.result(Future.sequence(futures), Duration.Inf)

    logger.info("Done")
  }

  def readVcfFile(cmdArgs: Args, sampleMap: Broadcast[Map[String, Int]])(
      implicit sc: SparkContext): RDD[VariantCall] = {
    val dict = sc.broadcast(fasta.getCachedDict(cmdArgs.reference))
    val regions =
      BedRecordList.fromReference(cmdArgs.reference).scatter(50000000)
    sc.parallelize(regions).mapPartitions { it =>
      it.flatMap { list =>
        vcf
          .loadRegions(cmdArgs.inputFile, list.iterator)
          .map(VariantCall.fromVariantContext(_, dict.value, sampleMap.value))
      }
    }
  }

  def readBamFile(cmdArgs: Args,
                  correctCells: Broadcast[Array[String]],
                  correctCellsMap: Broadcast[Map[String, Int]])(
      implicit sc: SparkContext): CellVariantcaller.Result = {
    logger.info(s"Starting variant calling on '${cmdArgs.inputFile}'")
    logger.info(
      s"Using default parameters, to set different cutoffs please use the CellVariantcaller module")
    val dict = sc.broadcast(bam.getDictFromBam(cmdArgs.inputFile))
    val partitions = {
      val x = (cmdArgs.inputFile.length() / 10000000).toInt
      if (x > 0) x else 1
    }
    val cutoffs = sc.broadcast(variantcalls.Cutoffs())
    val result = CellVariantcaller.totalRun(
      cmdArgs.inputFile,
      cmdArgs.outputDir,
      cmdArgs.reference,
      dict,
      partitions,
      cmdArgs.intervals,
      cmdArgs.sampleTag,
      cmdArgs.umiTag,
      correctCells,
      correctCellsMap,
      cutoffs,
      variantcalls.Args().seqError
    )

    result
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
