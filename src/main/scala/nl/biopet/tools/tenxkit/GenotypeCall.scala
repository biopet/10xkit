/*
 * Copyright (c) 2018 Biopet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.tenxkit

/**
  * A class to store genotype information for a single sample/cell
  * @param genotype Alleles of the sample, None means not called
  * @param ad Depth of each allele
  * @param aq Quality of each allele in phred
  * @param pl Pgred value for each allele and each possible expected alleles, for ploidy 2 that should be 3 values, 0,1,2
  */
case class GenotypeCall(genotype: IndexedSeq[Option[Int]],
                        ad: IndexedSeq[Int],
                        aq: IndexedSeq[Int],
                        pl: IndexedSeq[IndexedSeq[Int]])

object GenotypeCall {

  def probabilityToPhred(p: Double): Int = (-10 * math.log(p)).toInt

  private val t = 2.0

  /** This will calculate the probability of the number of alleles with a certain ploidy */
  def adToProbability(fraction: Double,
                      expectedAlleles: Int,
                      ploidy: Int = 2): Double = {
    val v = -math.pow(
      (fraction * (ploidy.toDouble / 2)) * t - ((t / 2.0) * expectedAlleles),
      2.0) + 1.0
    if (v > 1.0 || v < 0.0) 0.0 else v
  }

  /**
    * Generates a [[GenotypeCall]] from a given allele depths
    * @param ad Depth for each allele
    * @param ploidy ploidy to use
    * @return
    */
  def fromAd(ad: IndexedSeq[Int], ploidy: Int = 2,
             minAlleleDepth: Int = 1): GenotypeCall = {
    val totalDepth = ad.sum

    val probabilities = ad.zipWithIndex
      .map { case (d,i) => i -> d.toDouble / totalDepth}
      .map{case (i, f) =>
          i -> (0 to ploidy)
            .map(adToProbability(f, _, ploidy))}
    val pl = probabilities.map{ case (i,p) => p.map(probabilityToPhred) -> i}
    val plFilter = probabilities.map{ case (i,p) => p.map(probabilityToPhred) -> i}
      .filter { case (_, i) => ad(i) >= minAlleleDepth}

    def getGenotype(genotype: List[Option[Int]] = Nil): List[Option[Int]] = {
      val alleleLeft = ploidy - genotype.size
      if (alleleLeft == 0) genotype
      else {
        val filterPl = plFilter
          .filter{ case (_, alleleIdx) => !genotype.contains(Some(alleleIdx))}
          .map {
            case (aPl, aIdx) =>
              aIdx -> aPl.zipWithIndex.slice(1, alleleLeft + 1)
          }
        val flatten = filterPl.flatMap{case (_, pls) => pls.map{case (p, _) => p}}
        if (flatten.nonEmpty) {
          val min = flatten.min
          val newAlleles = filterPl
            .find{case (_, pls) => pls.map{case (p, _) => p}.contains(min)}
            .map{case (alleleIdx, pls) => alleleIdx -> pls.find{ case (p, _) => p == min}}
            .toList
            .flatMap {
              case (a, Some((_, n))) =>
                List.fill(n)(Some(a))
              case _ => Nil // Should never happen
            }
          getGenotype(newAlleles ::: genotype)
        } else genotype ::: List.fill(alleleLeft)(None)
      }
    }

    GenotypeCall(getGenotype().sorted.toIndexedSeq,
                 ad,
                 IndexedSeq(),
                 pl.map{ case (p, _) => p})
  }
}
