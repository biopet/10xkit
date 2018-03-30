package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import htsjdk.samtools.{
  SAMSequenceDictionary,
  SamReaderFactory,
  ValidationStringency
}
import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.tribble.index.IndexCreator
import htsjdk.variant.variantcontext.writer.{
  Options,
  VariantContextWriterBuilder
}
import htsjdk.variant.vcf._
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import nl.biopet.utils.ngs
import nl.biopet.utils.io
import nl.biopet.utils.ngs.bam
import nl.biopet.utils.ngs.intervals.BedRecordList
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{ReferenceRegion, VariantContext}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantContextRDD
import org.bdgenomics.adam.sql.{Variant, VariantCallingAnnotations}
import org.bdgenomics.formats.avro.Sample

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object CellVariantcaller extends ToolCommand[Args] {
  def argsParser: AbstractOptParser[Args] = new ArgsParser(this)
  def emptyArgs: Args = Args()

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    creatBamBins(cmdArgs.inputFile)

    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster)
    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val dict = sc.broadcast(bam.getDictFromBam(cmdArgs.inputFile))

    val correctCells =
      sc.broadcast(io.getLinesFromFile(cmdArgs.correctCells).toArray)
    require(correctCells.value.length == correctCells.value.distinct.length,
            "Duplicates cell barcodes found")
    val correctCellsMap = sc.broadcast(correctCells.value.zipWithIndex.toMap)
    val cutoffs = sc.broadcast(cmdArgs.cutoffs)

    val regions = (cmdArgs.intervals match {
      case Some(file) =>
        BedRecordList
          .fromFile(file)
          .combineOverlap
          .validateContigs(cmdArgs.reference)
      case _ => BedRecordList.fromReference(cmdArgs.reference)
    }).scatter(cmdArgs.binSize)

    val allVariants = sc
      .parallelize(regions, regions.size)
      .mapPartitions { it =>
        it.flatMap { x =>
          x.flatMap { region =>
            val samReader =
              SamReaderFactory.makeDefault().open(cmdArgs.inputFile)
            val fastaReader = new IndexedFastaSequenceFile(cmdArgs.reference)

            new ReadBam(
              samReader,
              cmdArgs.sampleTag,
              cmdArgs.umiTag,
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
      .cache()

    val headerLines: Seq[VCFHeaderLine] = Seq(
      new VCFInfoHeaderLine("DP", 1, VCFHeaderLineType.Integer, "Umi dept"),
      new VCFInfoHeaderLine("DP-READ",
                            1,
                            VCFHeaderLineType.Integer,
                            "Read dept"),
      new VCFInfoHeaderLine("SN", 1, VCFHeaderLineType.Integer, "Sample count"),
      new VCFFormatHeaderLine("GT",
                              VCFHeaderLineCount.UNBOUNDED,
                              VCFHeaderLineType.String,
                              ""),
      new VCFFormatHeaderLine("DP", 1, VCFHeaderLineType.Integer, "Total umi"),
      new VCFFormatHeaderLine("DP-READ",
                              1,
                              VCFHeaderLineType.Integer,
                              "Total reads"),
      new VCFFormatHeaderLine("DPF",
                              1,
                              VCFHeaderLineType.Integer,
                              "Forward umi"),
      new VCFFormatHeaderLine("DPR",
                              1,
                              VCFHeaderLineType.Integer,
                              "Reverse umi"),
      new VCFFormatHeaderLine("SEQ-ERR",
                              VCFHeaderLineCount.R,
                              VCFHeaderLineType.Float,
                              "Seq error of possible allele"),
      new VCFFormatHeaderLine("AD",
                              VCFHeaderLineCount.R,
                              VCFHeaderLineType.Integer,
                              "Total umi count per allele"),
      new VCFFormatHeaderLine("AD-READ",
                              VCFHeaderLineCount.R,
                              VCFHeaderLineType.Integer,
                              "Total reads count per allele"),
      new VCFFormatHeaderLine("ADF",
                              VCFHeaderLineCount.R,
                              VCFHeaderLineType.Integer,
                              "Forward umi count per allele"),
      new VCFFormatHeaderLine("ADR",
                              VCFHeaderLineCount.R,
                              VCFHeaderLineType.Integer,
                              "Reverse umi count per allele")
    )

    val vcfHeader =
      sc.broadcast(new VCFHeader(headerLines.toSet, correctCells.value.toSet))

    val filteredVariants = allVariants
      .flatMap(
        _.setAllelesToZeroPvalue(cmdArgs.seqError, cmdArgs.cutoffs.maxPvalue)
          .setAllelesToZeroDepth(cutoffs.value.minCellAlternativeDepth)
          .cleanupAlleles())
      .filter(
        x =>
          x.hasNonReference &&
            x.altDepth >= cutoffs.value.minAlternativeDepth &&
            x.totalDepth >= cutoffs.value.minTotalDepth &&
            x.minSampleAltDepth(cutoffs.value.minCellAlternativeDepth))

    writeVcf(filteredVariants,
             new File(cmdArgs.outputDir, "filter-vcf"),
             correctCells,
             dict,
             vcfHeader,
             cmdArgs.seqError)

    writeVcf(allVariants,
             new File(cmdArgs.outputDir, "raw-vcf"),
             correctCells,
             dict,
             vcfHeader,
             cmdArgs.seqError)

    logger.info("Done")
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

  def creatBamBins(bamFile: File) = {
    val samReader = SamReaderFactory.makeDefault().open(bamFile)
    val dict = samReader.getFileHeader.getSequenceDictionary
    val index = samReader.indexing().getIndex
    val chunksEachContig = for (seq <- dict.getSequences) yield {
      seq -> index.getSpanOverlapping(seq.getSequenceIndex, 0, seq.getSequenceLength).getChunks
    }
    ""
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
