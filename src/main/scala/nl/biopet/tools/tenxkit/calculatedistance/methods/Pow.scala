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

class Pow(pow: Double) extends Method {
  protected def calulateMethod(cell1: Array[Int], cell2: Array[Int]): Double = {
    val cell1Total = cell1.sum
    val cell2Total = cell2.sum
    cell1
      .zip(cell2)
      .map {
        case (a1, a2) =>
          val f1 = a1.toDouble / cell1Total
          val f2 = a2.toDouble / cell2Total
          val midlePoint = ((f1 - f2) / 2) + f1
          val distanceToMidle = math.sqrt(math.pow(midlePoint - f1, 2) * 2)
          math.pow(distanceToMidle, pow)
      }
      .sum
  }
}
