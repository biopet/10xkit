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

case class GenotypeCall(genotype: Array[Option[Int]],
                        ad: Array[Int],
                        aq: Array[Int],
                        pl: Array[Array[Int]])

object GenotypeCall {

  def probabilityToPhred(p: Double): Int = (-10 * math.log(p)).toInt

  val t = 2.0

  def adToProbability(fraction: Double,
                      expectedAlleles: Int,
                      ploidy: Int = 2): Double = {
    val v = -math.pow(
      (fraction * (ploidy.toDouble / 2)) * t - ((t / 2.0) * expectedAlleles),
      2.0) + 1.0
    if (v > 1.0 || v < 0.0) 0.0 else v
  }

  def fromAd(ad: Array[Int], ploidy: Int = 2): GenotypeCall = {
    val totalDepth = ad.sum

    val probabilities = ad
      .map(_.toDouble / totalDepth)
      .map(
        f =>
          (0 to ploidy)
            .map(adToProbability(f, _, ploidy))
            .toArray)
    val pl = probabilities.map(_.map(probabilityToPhred))

    def getGenotype(genotype: List[Option[Int]] = Nil): List[Option[Int]] = {
      val alleleLeft = ploidy - genotype.size
      if (alleleLeft == 0) genotype
      else {
        val filterPl = pl.zipWithIndex
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
            .map{case (alleleIdx, pls) => alleleIdx -> pls.find{ case (p, _) => p == min}.get}
            .toList
            .flatMap {
              case (a, (_, n)) =>
                List.fill(n)(Some(a))
            }
          getGenotype(genotype ::: newAlleles)
        } else genotype ::: List.fill(alleleLeft)(None)
      }
    }

    val min = pl.tail.flatten.min

    GenotypeCall(getGenotype().sorted.toArray,
                 ad,
                 Array(),
                 pl.map(_.map(_ - min)))
  }
}
