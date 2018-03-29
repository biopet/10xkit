package nl.biopet.tools.tenxkit.variantcalls

import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.variantcontext.{
  Allele,
  GenotypeBuilder,
  VariantContext,
  VariantContextBuilder
}

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

  def totalReadDepth: Int = {
    samples.map(_._2.map(_.totalReads).sum).sum
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
    s"${dict.getSequence(contig).getSequenceName}\t$pos\t.\t$refAllele\t${altAlleles
      .mkString(",")}\t.\t.\t.\tAD\t" +
      s"${samplesIdxs
        .map { idx =>
          samples.get(idx).map(a => s"${a.map(_.total).mkString(",")}").getOrElse(".")
        }
        .mkString("\t")}"
  }

  def toVariantContext(sampleList: Array[String],
                       dict: SAMSequenceDictionary): VariantContext = {
    val genotypes = samples.map {
      case (sample, a) =>
        val attributes = Map(
          "DP-READ" -> a.map(_.totalReads).sum.toString,
          "DPF" -> a.map(_.forwardUmi).sum.toString,
          "DPR" -> a.map(_.reverseUmi).sum.toString,
          "AD-READ" -> a.map(_.totalReads).mkString(","),
          "ADF" -> a.map(_.forwardUmi).mkString(","),
          "ADR" -> a.map(_.reverseUmi).mkString(",")
        )
        new GenotypeBuilder(sampleList(sample))
          .AD(a.map(_.total))
          .DP(a.map(_.total).sum)
          .attributes(attributes)
          .make()
    }
    val alleles = Allele.create(refAllele, true) :: altAlleles
      .map(Allele.create)
      .toList
    val attributes =
      Map("DP" -> totalDepth, "DP-READ" -> totalReadDepth, "SN" -> samples.size)
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

object VariantCall {
  def createFromBases(position:Int, contig:Int, bases: List[SampleBase]): VariantCall = {
    val end = position + allSamples.map(_._1.delBases).max
    val refAllele = fastaReader
      .getSubsequenceAt(
        dict.value.getSequence(contig).getSequenceName,
        position,
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

    bases.groupBy(_.sample).map { case (sample, sampleBases) =>
      sampleBases.groupBy(_.all)
    }
  }
}
