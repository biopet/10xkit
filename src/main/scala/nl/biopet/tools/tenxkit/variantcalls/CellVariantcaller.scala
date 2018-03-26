package nl.biopet.tools.tenxkit.variantcalls

import java.io.{File, PrintWriter}

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import nl.biopet.utils.io
import nl.biopet.utils.ngs.bam
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.sql.{Genotype, Variant}

import scala.collection.JavaConversions._

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

    val allReads = sc.loadIndexedBam(cmdArgs.inputFile.getAbsolutePath, contigs)

    val filteredReads = for (contig <- contigs) yield {
      val contigName = contig.referenceName
      logger.info(s"Start on contig '$contigName'")
      sc.setJobGroup(s"Contig: $contigName", s"Contig: $contigName")
//      val contigReads: AlignmentRecordRDD =
//        sc.loadIndexedBam(cmdArgs.inputFile.getAbsolutePath, contigRegion)
      val contigReads = allReads.filterByOverlappingRegion(contig)

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

      val bla = bases.aggregateByKey(Map[Key, Value]())({ case (a, b) =>
        val key = Key(b.allele, b.delBases)
        a.get(key) match {
          case Some(count) if b.strand => a + (key -> count.copy(forward = count.forward + 1))
          case Some(count) => a + (key -> count.copy(reverse = count.reverse + 1))
          case _ if b.strand => a + (key -> Value(forward = 1))
          case _ => a + (key -> Value(reverse = 1))
        }
      }, { case (a, b) =>
          val keys = a.keySet ++ b.keySet
          keys.map { key =>
            (a.get(key), b.get(key)) match {
              case (Some(x), Some(y)) => key -> Value(x.forward + y.forward, x.reverse + y.reverse)
              case (Some(x), _) => key -> x
              case (_, Some(y)) => key -> y
            }
          }.toMap
      }).mapPartitions { it =>
        val fastaReader = new IndexedFastaSequenceFile(cmdArgs.reference)
        it.map {
          case (samplePos, list) =>
            val end = samplePos.position + list.map(_._1.delBases).max
            val refAllele = fastaReader
              .getSubsequenceAt(contigName, samplePos.position, end)
              .getBaseString
            val alleles = list
              .map {
                case (key, bases) =>
                  val newAllele =
                    if (key.delBases > 0 || !refAllele.startsWith(key.allele)) {
                      if (key.allele.length == refAllele.length || key.delBases > 0)
                        key.allele
                      else
                        new String(refAllele.zipWithIndex
                          .map(x => key.allele.lift(x._2).getOrElse(x._1))
                          .toArray)
                    } else refAllele
                  AlleleCount(newAllele,
                    bases.forward,
                    bases.reverse,
                    newAllele == refAllele)
              }
              .toList
            val newAlleles =
              if (alleles.exists(_.allele == refAllele)) alleles
              else AlleleCount(refAllele, 0, 0, true) :: alleles
            samplePos.position -> SampleVariant(samplePos.sample, newAlleles)
        }
      }

      val ds = bla.groupByKey()
        .map {
          case (pos, list) =>
            VariantCall.from(list.toList, contigName, pos)
        }
        .filter(_.hasNonReference)
        .filter(_.altDepth >= cmdArgs.minAlternativeDepth)
        .filter(_.totalDepth >= cmdArgs.minTotalDepth)
        .filter(_.minSampleAltDepth(cmdArgs.minCellAlternativeDepth))
        .toDS()
        .cache()
      //ds.rdd.countAsync()
      sc.clearJobGroup()
      ds
    }

    filteredReads
      .reduce(_.union(_))
      .toDF()
      .map(_.toString())
      .write
      .csv(new File(cmdArgs.outputDir, "counts.tsv").getAbsolutePath)

    logger.info("Done")
  }

  case class Value(forward: Int = 0, reverse: Int = 0)
  case class Key(allele: String, delBases: Int)

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
