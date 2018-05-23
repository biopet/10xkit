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

package nl.biopet.tools.tenxkit.calculatedistance.methods

trait Method extends Serializable {

  /**
    * Calculate a single distance for alleles from the same position
    * @param cell1 Alleles from cell 1
    * @param cell2 alleles from cell 2
    * @return
    */
  final def calculate(cell1: IndexedSeq[Int], cell2: IndexedSeq[Int]): Double = {
    require(cell1.length == cell2.length, "not the same count of alleles")
    calulateInternal(cell1, cell2)
  }

  protected def calulateInternal(cell1: IndexedSeq[Int], cell2: IndexedSeq[Int]): Double
}

object Method {

  /** Parsing a argument string to a specfic [[Method]] */
  def fromString(string: String): Method = {
    string match {
      case s if s.startsWith("pow") => new Pow(s.stripPrefix("pow").toDouble)
      case s if s.startsWith("depthpow") =>
        new DepthPow(s.stripPrefix("depthpow").toDouble)
      case "chi2" => new Chi2
      case s if s.startsWith("chi2pow") =>
        val values = s.stripPrefix("chi2pow").split("-").map(_.toDouble)
        new Chi2Pow(values(0), values(1))
      case _ =>
        throw new IllegalArgumentException(s"Method '$string' does not exist")
    }
  }
}
