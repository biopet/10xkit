package nl.biopet.tools.tenxkit.variantcalls

import htsjdk.samtools.SAMSequenceDictionary

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
}
