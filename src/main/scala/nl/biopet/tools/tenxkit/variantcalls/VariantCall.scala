package nl.biopet.tools.tenxkit.variantcalls

case class VariantCall(contig: String, pos: Long, samples: Map[String, List[AlleleCount]]) {
  def hasNonReference: Boolean = {
    samples.exists(_._2.exists(!_.reference))
  }

  def totalDepth: Int = {
    samples.map(_._2.map(_.total).sum).sum
  }

  def altDepth: Int = {
    samples.map(_._2.filter(!_.reference).map(_.total).sum).sum
  }

  def minSampleAltDepth(cutoff: Int): Boolean = {
    samples.values.exists(_.filter(!_.reference).exists(_.total >= cutoff))
  }
}

object VariantCall {
  def from(list: List[SampleVariant], contig: String, pos: Long): VariantCall = {
    require(list.map(_.pos).distinct.size == 1, "Can't merge different positions")
    val refAlleles = list.flatMap(_.alleles.find(_.reference == true).map(_.allele)).distinct

    val refAllele = (if (refAlleles.size > 1) {
      refAlleles.find(_.length == refAlleles.map(x => x.length).max)
    } else refAlleles.headOption) match {
      case Some(x) => x
      case _ => throw new IllegalStateException("No ref allele found")
    }

    val sampleVariants = list.map { sampleVariant =>
      val split = sampleVariant.alleles.groupBy(_.reference)
      sampleVariant.sample -> (if (split(true).head.allele == refAllele) {
        sampleVariant.alleles
      } else {
        sampleVariant.alleles.map { x =>
          x.copy(allele = new String(refAllele.zipWithIndex.map(y => x.allele.lift(y._2).getOrElse(y._1)).toArray))
        }
      })
    }.toMap

    VariantCall(contig, pos, sampleVariants)
  }
}