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

import java.io.{File, PrintWriter}

import nl.biopet.utils.Logging

import scala.io.Source

case class DistanceMatrix(values: Array[Array[Option[Double]]],
                          samples: Array[String])
    extends Logging {

  def apply(s1: Int, s2: Int): Option[Double] = {
    val v1 = values(s1)(s2)
    val v2 = values(s2)(s1)
    (v1, v2) match {
      case (Some(v), _) => Some(v)
      case (_, Some(v)) => Some(v)
      case _            => None
    }
  }

  def overlapSample(s1: Int, s2: Int, cutoff: Double): Boolean = {
    this(s1, s2).forall(_ <= cutoff)
  }

  def overlapSamples(sample: Int, cutoff: Double): Array[Int] = {
    samples.indices
      .filter(overlapSample(sample, _, cutoff))
      .toArray
  }

  def wrongSamples(sample: Int, cutoff: Double): Array[Int] = {
    samples.indices
      .filterNot(overlapSample(sample, _, cutoff))
      .toArray
  }

  def extractSamples(extractSamples: List[String]): DistanceMatrix = {
    val newSamples =
      samples.zipWithIndex.filter { case (name, idx) => extractSamples.contains(name) }
    val idxs = newSamples.map{ case (_, idx) => idx }
    val newValues = for (s1 <- idxs) yield {
      for (s2 <- idxs) yield {
        values(s1)(s2)
      }
    }
    DistanceMatrix(newValues, newSamples.map { case (name, _) => name })
  }

  def writeFile(file: File): Unit = {
    val writer =
      new PrintWriter(file)
    writer.println(samples.mkString("Sample\t", "\t", ""))
    for ((list, s1) <- values.zipWithIndex) {
      writer.print(s"${samples(s1)}\t")
      writer.println(list.map(_.getOrElse(".")).mkString("\t"))
    }
    writer.close()
  }

  def totalHistogram: DistanceHistogram = {
    val histogram = new DistanceHistogram
    values.foreach(_.foreach(_.foreach(histogram.add)))
    histogram
  }

  case class SubgroupHistogram(group: DistanceHistogram,
                               nonGroup: DistanceHistogram)

  def subgroupHistograms(barcodes1: List[Int],
                         barcodes2: List[Int]): DistanceHistogram = {
    val histogram = new DistanceHistogram

    for {
      s1 <- barcodes1
      s2 <- barcodes2
    } {
      values.lift(s1).foreach(_.lift(s2).flatten.foreach(histogram.add))
      values.lift(s2).foreach(_.lift(s1).flatten.foreach(histogram.add))
    }
    histogram
  }

  def subGroupDistance(samples: List[Int]): Double = {
    (for {
      s1 <- samples
      s2 <- samples
    } yield this(s1, s2)).flatten.sum / samples.size
  }

  def subGroupDistance(sample: Int, samples: List[Int]): Double = {
    samples.flatMap(this(_, sample)).sum / samples.size
  }
}

object DistanceMatrix extends Logging {
  def fromFile(file: File): DistanceMatrix = {
    val reader = Source.fromFile(file)
    val readerIt = reader.getLines()

    val samples = readerIt.next().split("\t").tail
    val sampleMap = samples.zipWithIndex.toMap

    val data = (for ((line, idx) <- readerIt.zipWithIndex) yield {
      val values = line.split("\t")
      require(sampleMap(values.head) == idx,
              "Order of rows is different then columns")
      values.tail.map(x => if (x == ".") None else Some(x.toDouble))
    }).toArray
    reader.close()
    DistanceMatrix(data, samples)
  }
}
