package nl.biopet.tools

import java.io.File

import htsjdk.variant.vcf._
import nl.biopet.utils.io
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._

package object tenxkit {
  lazy val headerLines: Seq[VCFHeaderLine] = Seq(
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
      "Forward strand umi"),
    new VCFFormatHeaderLine("DPR",
      1,
      VCFHeaderLineType.Integer,
      "Reverse strand umi"),
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
    new VCFFormatHeaderLine("ADF-READ",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Forward strand reads count per allele"),
    new VCFFormatHeaderLine("ADR-READ",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Reverse strand reads count per allele"),
    new VCFFormatHeaderLine("ADF",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Forward strand umi count per allele"),
    new VCFFormatHeaderLine("ADR",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Reverse strand umi count per allele")
  )

  def vcfHeader(samples: Array[String]) = new VCFHeader(headerLines.toSet, samples.toSet)

  def parseCorrectCells(file: File)(implicit sc: SparkContext): Broadcast[Array[String]] = {
    val correctCells =
      sc.broadcast(io.getLinesFromFile(file).toArray)
    require(correctCells.value.length == correctCells.value.distinct.length,
      "Duplicates cell barcodes found")
    correctCells
  }

  def correctCellsMap(samples: Broadcast[Array[String]])(implicit sc: SparkContext): Broadcast[Map[String, Int]] = {
    sc.broadcast(samples.value.zipWithIndex.toMap)
  }
}
