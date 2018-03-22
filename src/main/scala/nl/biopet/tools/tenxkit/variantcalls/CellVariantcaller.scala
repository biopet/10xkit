package nl.biopet.tools.tenxkit.variantcalls

import java.io.{File, PrintWriter}

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import nl.biopet.utils.io
import nl.biopet.utils.ngs.fasta
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.sql.{Genotype, Variant}

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
                       read.getStart + 1,
                       read.getEnd,
                       read.getSequence.getBytes,
                       read.getQual.getBytes,
                       read.getCigar,
                       !read.getReadNegativeStrand))
      }
      .flatMap(_.sampleBases.filter(_.avgQual.exists(_ >= cmdArgs.minBaseQual)))
      .groupBy(x => (x.sample, x.contig, x.pos))
      .mapPartitions { it =>
        val fastaReader = new IndexedFastaSequenceFile(cmdArgs.reference)
        it.map { case ((sample, contig, pos),list) =>
          val end = pos + list.map(_.delBases).max
          val refAllele = fastaReader.getSubsequenceAt(contig, pos, end).getBaseString
          val alleles = list.groupBy(x => (x.allele, x.delBases)).map { case ((allele, delBases), bases) =>
            val newAllele = if (delBases > 0 || !refAllele.startsWith(allele)) {
              if (allele.length == refAllele.length || delBases > 0) allele
              else new String(refAllele.zipWithIndex.map(x => allele.lift(x._2).getOrElse(x._1)).toArray)
            } else refAllele
            val total = bases.size
            val forward = bases.count(_.strand)
            val reverse = total - forward
            AlleleCount(newAllele, forward, reverse, newAllele == refAllele)
          }.toList
          SampleVariant(sample, contig, pos, if (alleles.exists(_.allele == refAllele)) alleles else AlleleCount(refAllele, 0, 0, true) :: alleles)
        }
    }
      .groupBy(x => (x.contig, x.pos)).map { case ((contig, pos), list) =>
      VariantCall.from(list.toList, contig, pos)
    }
      .filter(_.hasNonReference)
      .filter (_.altDepth >= cmdArgs.minAlternativeDepth)
      .filter (_.totalDepth >= cmdArgs.minTotalDepth)
      .filter(_.minSampleAltDepth(cmdArgs.minCellAlternativeDepth))
      .toDS()
      //.cache()

    //val writer = new PrintWriter(new File(cmdArgs.outputDir, "counts.tsv"))
    val df = filteredReads.toDF().map(_.toString()).write.csv(new File(cmdArgs.outputDir, "counts.tsv").getAbsolutePath)
//      .groupBy("sample", "contig", "pos", "allele", "delBases")
//      .count()
//    writer.println(df.columns.mkString("#","\t", ""))
//    df.collect()
//      .foreach(row => writer.println(row.mkString("\t")))
//    writer.close()

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
