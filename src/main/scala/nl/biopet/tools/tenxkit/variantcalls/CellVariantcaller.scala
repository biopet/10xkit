package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import nl.biopet.utils.io
import nl.biopet.utils.ngs.bam
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext._

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

    val dict = bam.getDictFromBam(cmdArgs.inputFile)

    val correctCells =
      sc.broadcast(io.getLinesFromFile(cmdArgs.correctCells).toArray)
    require(correctCells.value.length == correctCells.value.distinct.length,
            "Duplicates cell barcodes found")
    val correctCellsMap = sc.broadcast(correctCells.value.zipWithIndex.toMap)
    val minBaseQual = sc.broadcast(cmdArgs.minBaseQual)

    val contigs = dict.getSequences.map(c =>
      ReferenceRegion(c.getSequenceName, 1, c.getSequenceLength))

    val filteredReads = for (contig <- contigs) yield {
      val contigName = contig.referenceName
      logger.info(s"Start on contig '$contigName'")
      sc.setJobGroup(s"Contig: $contigName", s"Contig: $contigName")
      val contigReads: AlignmentRecordRDD =
        sc.loadIndexedBam(cmdArgs.inputFile.getAbsolutePath, contig)

      val bases = contigReads.rdd.flatMap { read =>
        val sample = read.getAttributes
          .split("\t")
          .find(_.startsWith(cmdArgs.sampleTag + ":"))
          .flatMap(t => correctCellsMap.value.get(t.split(":")(2)))
        sample.toList
          .flatMap(
            s =>
              SampleRead.sampleBases(read.getStart + 1,
                                     s,
                                     !read.getReadNegativeStrand,
                                     read.getSequence.getBytes,
                                     read.getQual.getBytes,
                                     read.getCigar))
          .filter(_._2.avgQual.exists(_ >= minBaseQual.value))
      }

      val bla = bases
        .aggregateByKey(Map[Key, AlleleCount]())(
          {
            case (a, b) =>
              val key = Key(b.sample, b.allele, b.delBases)
              a.get(key) match {
                case Some(count) if b.strand =>
                  a + (key -> count.copy(forward = count.forward + 1))
                case Some(count) =>
                  a + (key -> count.copy(reverse = count.reverse + 1))
                case _ if b.strand => a + (key -> AlleleCount(forward = 1))
                case _             => a + (key -> AlleleCount(reverse = 1))
              }
          }, {
            case (a, b) =>
              val keys = a.keySet ++ b.keySet
              keys.map { key =>
                (a.get(key), b.get(key)) match {
                  case (Some(x), Some(y)) =>
                    key -> AlleleCount(x.forward + y.forward,
                                       x.reverse + y.reverse)
                  case (Some(x), _) => key -> x
                  case (_, Some(y)) => key -> y
                }
              }.toMap
          }
        )
      val ds = bla
        .mapPartitions { it =>
          val fastaReader = new IndexedFastaSequenceFile(cmdArgs.reference)
          it.map {
            case (position, allSamples) =>
              val end = position + allSamples.map(_._1.delBases).max
              val refAllele = fastaReader
                .getSubsequenceAt(contigName, position, end)
                .getBaseString
                .toUpperCase
              val oldAlleles = allSamples.keys.map(x => (x.allele, x.delBases))
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
                      .find(x =>
                        newAllelesMap((x._1.allele, x._1.delBases)) == allele)
                      .map(x => x._2)
                      .getOrElse(AlleleCount())
                  }
              }
              VariantCall(contigName,
                          position,
                          refAllele,
                          altAlleles,
                          genotypes)
          }
        }
        .filter(_.hasNonReference)
        .filter(_.altDepth >= cmdArgs.minAlternativeDepth)
        .filter(_.totalDepth >= cmdArgs.minTotalDepth)
        .filter(_.minSampleAltDepth(cmdArgs.minCellAlternativeDepth))
        .toDS()
        .cache()
      ds.rdd.countAsync()

      sc.clearJobGroup()
      contigName -> ds
    }

    val futures = filteredReads.map {
      case (contigName, ds) =>
        Future {
          ds.map(_.toVcfLine(correctCells.value.indices))
            .write
            .text(new File(cmdArgs.outputDir,
                           "vcf" + File.separator + contigName).getAbsolutePath)
          ds.unpersist()
        }
    }

    Await.result(Future.sequence(futures), Duration.Inf)

//    filteredReads.map(_._2)
//      .reduce(_.union(_))
//      .map(_.toVcfLine(correctCells.value.indices))
//      .write
//      .text(new File(cmdArgs.outputDir, "counts.tsv").getAbsolutePath)

    logger.info("Done")
  }

  case class Key(sample: Int, allele: String, delBases: Int)

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
