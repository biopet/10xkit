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

    val contigs = dict.getSequences.map(c => ReferenceRegion(c.getSequenceName, 1, c.getSequenceLength))

    val allReads = sc.loadIndexedBam(cmdArgs.inputFile.getAbsolutePath, contigs)

    val filteredReads = for (contig <- contigs) yield {
      val contigName = contig.referenceName
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

      val ds = bases
        .groupByKey()
        .mapPartitions { it =>
          val fastaReader = new IndexedFastaSequenceFile(cmdArgs.reference)
          it.map {
            case (samplePos, list) =>
              val end = samplePos.position + list.map(_.delBases).max
              val refAllele = fastaReader
                .getSubsequenceAt(contigName, samplePos.position, end)
                .getBaseString
              val alleles = list
                .groupBy(x => (x.allele, x.delBases))
                .map {
                  case ((allele, delBases), bases) =>
                    val newAllele =
                      if (delBases > 0 || !refAllele.startsWith(allele)) {
                        if (allele.length == refAllele.length || delBases > 0)
                          allele
                        else
                          new String(refAllele.zipWithIndex
                            .map(x => allele.lift(x._2).getOrElse(x._1))
                            .toArray)
                      } else refAllele
                    val total = bases.size
                    val forward = bases.count(_.strand)
                    val reverse = total - forward
                    AlleleCount(newAllele,
                                forward,
                                reverse,
                                newAllele == refAllele)
                }
                .toList
              val newAlleles =
                if (alleles.exists(_.allele == refAllele)) alleles
                else AlleleCount(refAllele, 0, 0, true) :: alleles
              samplePos.position -> SampleVariant(samplePos.sample, newAlleles)
          }
        }
        .groupByKey()
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
