package nl.biopet.tools.tenxkit.variantcalls

import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContext, VariantContextBuilder}
import org.bdgenomics.adam.converters.VariantContextConverter

import scala.collection.JavaConversions._

case class VariantCall(contig: Int,
                       pos: Long,
                       refAllele: String,
                       altAlleles: Array[String],
                       samples: Map[Int, Array[AlleleCount]]) {
  def hasNonReference: Boolean = {
    samples.exists(_._2.length > 1)
  }

  def totalDepth: Int = {
    samples.map(_._2.map(_.total).sum).sum
  }

  def referenceDepth: Int = {
    samples.map(_._2.headOption.map(_.total).getOrElse(0)).sum
  }

  def altDepth: Int = {
    totalDepth - referenceDepth
  }

  def minSampleAltDepth(cutoff: Int): Boolean = {
    samples.values.exists(_.tail.exists(_.total >= cutoff))
  }

  def toVcfLine(samplesIdxs: Range, dict: SAMSequenceDictionary): String = {
    s"${dict.getSequence(contig).getSequenceName}\t$pos\t.\t$refAllele\t${altAlleles.mkString(",")}\t.\t.\t.\tAD\t" +
      s"${samplesIdxs
        .map { idx =>
          samples.get(idx).map(a => s"${a.map(_.total).mkString(",")}").getOrElse(".")
        }
        .mkString("\t")}"
  }

  def toVariantContext(sampleList: Array[String],
                       dict: SAMSequenceDictionary): VariantContext = {
    val genotypes = samples.map { case (sample, a) =>
      val attributes = Map(
        "DPF" -> a.map(_.forward).sum.toString,
        "DPR" -> a.map(_.reverse).sum.toString,
        "ADF" -> a.map(_.forward).mkString(","),
        "ADR" -> a.map(_.reverse).mkString(",")
      )
      new GenotypeBuilder(sampleList(sample)).AD(a.map(_.total)).DP(a.map(_.total).sum).attributes(attributes).make()
    }
    val alleles = Allele.create(refAllele, true) :: altAlleles.map(Allele.create).toList
    val attributes = Map("DP" -> totalDepth, "SN" -> samples.size)
    new VariantContextBuilder()
      .chr(dict.getSequence(contig).getSequenceName)
      .start(pos)
      .attributes(attributes)
      .computeEndFromAlleles(alleles, pos.toInt)
      .alleles(alleles)
      .genotypes(genotypes)
      .make()
  }
}
