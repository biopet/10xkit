package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder
import htsjdk.variant.vcf._
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import nl.biopet.utils.ngs
import nl.biopet.utils.io
import nl.biopet.utils.ngs.bam
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
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

    val allReads = sc.loadBam(cmdArgs.inputFile.getAbsolutePath)

//    val contigs = dict.value.getSequences.map(c =>
//      ReferenceRegion(c.getSequenceName, 1, c.getSequenceLength))
//
//    val filteredReads = for (contig <- contigs) yield {
//      val contigName = contig.referenceName
//      logger.info(s"Start on contig '$contigName'")
//      sc.setJobGroup(s"Contig: $contigName", s"Contig: $contigName")
//      val contigReads: AlignmentRecordRDD =
//        sc.loadIndexedBam(cmdArgs.inputFile.getAbsolutePath, contig)

    val bases = allReads.rdd
      .filter(r =>
        r.getReadMapped && (cmdArgs.umnTag.isDefined || !r.getDuplicateRead))
      .flatMap { read =>
        val attributes = read.getAttributes
          .split("\t")
        val sample = attributes
          .find(_.startsWith(cmdArgs.sampleTag + ":"))
          .flatMap(t => correctCellsMap.value.get(t.split(":")(2)))

        sample.toList
          .flatMap { s =>
            val umni = cmdArgs.umnTag.flatMap { u =>
              attributes
                .find(_.startsWith(u + ":"))
                .map(_.split(":")(2))
                .map(ngs.sequenceTo2bitSingleInt)
            }
            SampleBase.createBases(
              dict.value.getSequenceIndex(read.getContigName),
              read.getStart + 1,
              s,
              !read.getReadNegativeStrand,
              read.getSequence.getBytes,
              read.getQual.getBytes,
              read.getCigar,
              umni
            )
          }
          .filter(_._2.avgQual.exists(_ >= cutoffs.value.minBaseQual))
      }

    val ds = bases
      .aggregateByKey(Map[Key, AlleleCount]())(
        {
          case (a, b) =>
            val key = Key(b.sample, b.allele, b.delBases, b.umi)
            a.get(key) match {
              case Some(count) if b.strand =>
                a + (key -> count.addForward())
              case Some(count) =>
                a + (key -> count.addReverse())
              case _ if b.strand => a + (key -> AlleleCount(forward = 1))
              case _             => a + (key -> AlleleCount(reverse = 1))
            }
        }, {
          case (a, b) =>
            val keys = a.keySet ++ b.keySet
            keys.map { key =>
              (a.get(key), b.get(key)) match {
                case (Some(x), Some(y)) =>
                  key -> (x + y)
                case (Some(x), _) => key -> x
                case (_, Some(y)) => key -> y
              }
            }.toMap
        }
      )
      .mapPartitions { it =>
        val fastaReader = new IndexedFastaSequenceFile(cmdArgs.reference)
        it.map {
          case (position, allSamples) =>
            val end = position.position + allSamples.map(_._1.delBases).max
            val refAllele = fastaReader
              .getSubsequenceAt(
                dict.value.getSequence(position.contig).getSequenceName,
                position.position,
                end)
              .getBaseString
              .toUpperCase
            val oldAlleles =
              allSamples.keys.map(x => (x.allele, x.delBases)).toList.distinct
            val newAllelesMap = oldAlleles.map {
              case (allele, delBases) =>
                (allele, delBases) -> (if (delBases > 0 || !refAllele
                                             .startsWith(allele)) {
                                         if (allele.length == refAllele.length || delBases > 0)
                                           allele
                                         else
                                           new String(
                                             refAllele.zipWithIndex
                                               .map(x =>
                                                 allele
                                                   .lift(x._2)
                                                   .getOrElse(x._1))
                                               .toArray)
                                       } else refAllele)
            }.toMap
            val altAlleles =
              newAllelesMap.values.filter(_ != refAllele).toArray.distinct
            val allAlleles = Array(refAllele) ++ altAlleles
            val genotypes = allSamples.groupBy(_._1.sample).map {
              case (sample, list) =>
                sample -> allAlleles.map { allele =>
                  list
                    .filter(x =>
                      newAllelesMap((x._1.allele, x._1.delBases)) == allele)
                    .groupBy(_._1.umi)
                    .map {
                      case (k, v) =>
                        if (k.isDefined)
                          v.values
                            .foldLeft(AlleleCount())(_ + _)
                            .copy(forwardUmi = 1, reverseUmi = 1)
                        else
                          v.values
                            .foldLeft(AlleleCount())(_ + _)
                    }
                    .foldLeft(AlleleCount())(_ + _)
                }
            }
            VariantCall(position.contig,
                        position.position,
                        refAllele,
                        altAlleles,
                        genotypes)
        }
      }
      .filter(x =>
        x.hasNonReference &&
          x.altDepth >= cutoffs.value.minAlternativeDepth &&
          x.totalDepth >= cutoffs.value.minTotalDepth &&
          x.minSampleAltDepth(cutoffs.value.minCellAlternativeDepth))

//        .cache()
//      ds.rdd.countAsync()
//
//      sc.clearJobGroup()
//      contigName -> ds
//    }
//
//    val futures = filteredReads.map {
//      case (contigName, ds) =>
//        Future {
//          ds.map(_.toVcfLine(correctCells.value.indices))
//            .write
//            .text(new File(cmdArgs.outputDir,
//                           "vcf" + File.separator + contigName).getAbsolutePath)
//          ds.unpersist()
//        }
//    }
//
//    Await.result(Future.sequence(futures), Duration.Inf)

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

    val bla = ds
      .sortBy(x => (x.contig, x.pos))
      .map(_.toVariantContext(correctCells.value, dict.value))

    val vcfDir = new File(cmdArgs.outputDir, "vcf")
    vcfDir.mkdir()
    val outputFiles = bla
      .mapPartitionsWithIndex {
        case (idx, it) =>
          val outputFile = new File(vcfDir, s"$idx.vcf.gz")
          val writer =
            new VariantContextWriterBuilder().setOutputFile(outputFile).build()
          writer.writeHeader(vcfHeader.value)
          it.foreach(writer.add)
          writer.close()
          Iterator(outputFile)
      }
      .collect()

    logger.info("Done")
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
