package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import htsjdk.samtools._
import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.variant.variantcontext.writer.{
  Options,
  VariantContextWriterBuilder
}
import htsjdk.variant.vcf._
import nl.biopet.tools.tenxkit
import nl.biopet.utils.io
import nl.biopet.utils.ngs.bam
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.ngs.bam.IndexScattering._
import nl.biopet.utils.ngs.intervals.{BedRecord, BedRecordList}
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

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
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val dict = sc.broadcast(bam.getDictFromBam(cmdArgs.inputFile))

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val cutoffs = sc.broadcast(cmdArgs.cutoffs)
    val partitions = {
      val x = cmdArgs.partitions.getOrElse(
        (cmdArgs.inputFile.length() / 10000000).toInt)
      if (x > 0) x else 1
    }

    val result = totalRun(
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
      cmdArgs.seqError,
      true,
      true
    )

    Await.result(result.totalFuture, Duration.Inf)

    logger.info("Done")
  }

  case class Result(filteredVariants: Future[RDD[VariantCall]],
                    allVariants: RDD[VariantCall],
                    writeFilteredFuture: Option[Future[Unit]],
                    writeRawFuture: Option[Future[Unit]]) {
    def totalFuture: Future[List[Unit]] =
      Future.sequence(writeFilteredFuture.toList ::: writeRawFuture.toList)
  }

  def totalRun(
      inputFile: File,
      outputDir: File,
      reference: File,
      dict: Broadcast[SAMSequenceDictionary],
      partitions: Int,
      intervals: Option[File],
      sampleTag: String,
      umiTag: Option[String],
      correctCells: Broadcast[Array[String]],
      correctCellsMap: Broadcast[Map[String, Int]],
      cutoffs: Broadcast[Cutoffs],
      seqError: Float,
      writeRawVcf: Boolean = false,
      writeFilteredVcf: Boolean = true)(implicit sc: SparkContext): Result = {
    val regions = createRegions(inputFile, reference, partitions, intervals)

    val allVariants = createAllVariants(inputFile,
                                        reference,
                                        regions,
                                        correctCellsMap,
                                        cutoffs,
                                        sampleTag,
                                        umiTag)

    val vcfHeader = sc.broadcast(tenxkit.vcfHeader(correctCells.value))

    val filteredVariants =
      filterVariants(allVariants, seqError, cutoffs).map(_.cache())

    val writeFilterVcfFuture =
      if (writeFilteredVcf) Some(filteredVariants.map { rdd =>
        writeVcf(rdd.cache(),
                 new File(outputDir, "filter-vcf"),
                 correctCells,
                 dict,
                 vcfHeader,
                 seqError)
      })
      else None

    val writeAllVcfFuture = {
      if (writeRawVcf) Some(Future {
        writeVcf(allVariants.cache(),
                 new File(outputDir, "raw-vcf"),
                 correctCells,
                 dict,
                 vcfHeader,
                 seqError)
      })
      else None
    }

    Result(filteredVariants,
           allVariants,
           writeFilterVcfFuture,
           writeAllVcfFuture)
  }

  def filterVariants(variants: RDD[VariantCall],
                     seqError: Float = Args().seqError,
                     cutoffs: Broadcast[Cutoffs]): Future[RDD[VariantCall]] = {
    Future {
      variants
        .flatMap(
          _.setAllelesToZeroPvalue(seqError, cutoffs.value.maxPvalue)
            .setAllelesToZeroDepth(cutoffs.value.minCellAlternativeDepth)
            .cleanupAlleles())
        .filter(
          x =>
            x.hasNonReference &&
              x.altDepth >= cutoffs.value.minAlternativeDepth &&
              x.totalDepth >= cutoffs.value.minTotalDepth &&
              x.minSampleAltDepth(cutoffs.value.minCellAlternativeDepth))
        .sortBy(x => (x.contig, x.pos), numPartitions = 200)
        .cache()
    }
  }

  def createRegions(bamFile: File,
                    reference: File,
                    partitions: Int,
                    intervals: Option[File] = None): List[List[BedRecord]] = {
    intervals match {
      case Some(file) =>
        creatBamBins(BedRecordList
                       .fromFile(file)
                       .combineOverlap
                       .validateContigs(reference)
                       .allRecords
                       .toList,
                     bamFile,
                     partitions)
      case _ => creatBamBins(bamFile, partitions)
    }
  }

  def createAllVariants(inputFile: File,
                        reference: File,
                        regions: List[List[BedRecord]],
                        correctCellsMap: Broadcast[Map[String, Int]],
                        cutoffs: Broadcast[Cutoffs],
                        sampleTag: String = Args().sampleTag,
                        umiTag: Option[String] = Args().umiTag)(
      implicit sc: SparkContext): RDD[VariantCall] = {
    val dict = sc.broadcast(bam.getDictFromBam(inputFile))

    sc.parallelize(regions, regions.size)
      .mapPartitions { it =>
        it.flatMap { x =>
          x.flatMap { region =>
            val samReader =
              SamReaderFactory.makeDefault().open(inputFile)
            val fastaReader = new IndexedFastaSequenceFile(reference)

            new ReadBam(
              samReader,
              sampleTag,
              umiTag,
              region,
              dict.value,
              fastaReader,
              correctCellsMap.value,
              cutoffs.value.minBaseQual,
              cutoffs.value.minCellAlternativeDepth
            ).filter(
              x =>
                x.hasNonReference &&
                  x.altDepth >= cutoffs.value.minAlternativeDepth &&
                  x.totalDepth >= cutoffs.value.minTotalDepth &&
                  x.minSampleAltDepth(cutoffs.value.minCellAlternativeDepth))
          }
        }
      }
  }

  def writeVcf(rdd: RDD[VariantCall],
               outputDir: File,
               correctCells: Broadcast[Array[String]],
               dict: Broadcast[SAMSequenceDictionary],
               vcfHeader: Broadcast[VCFHeader],
               seqError: Float): Unit = {
    outputDir.mkdirs()
    val outputFiles = rdd
      .map(_.toVariantContext(correctCells.value, dict.value, seqError))
      .mapPartitionsWithIndex {
        case (idx, it) =>
          val outputFile = new File(outputDir, s"$idx.vcf.gz")
          val writer =
            new VariantContextWriterBuilder()
              .unsetOption(Options.INDEX_ON_THE_FLY)
              .setOutputFile(outputFile)
              .build()
          writer.writeHeader(vcfHeader.value)
          it.foreach(writer.add)
          writer.close()
          Iterator(outputFile)
      }
      .collect()
  }

  case class Key(sample: Int, allele: String, delBases: Int, umi: Option[Int])

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
