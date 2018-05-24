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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

case class DistanceMatrix(values: IndexedSeq[IndexedSeq[Option[Double]]],
                          samples: IndexedSeq[String])
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
      samples.zipWithIndex.filter {
        case (name, idx) => extractSamples.contains(name)
      }
    val idxs = newSamples.map { case (_, idx) => idx }
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
  def fromFile(file: File, countFile: Option[File] = None): DistanceMatrix = {
    val reader = Source.fromFile(file)
    val readerIt = reader.getLines()
    val countReader = countFile.map(Source.fromFile)
    val countIt = countReader.map(_.getLines())

    val samples = readerIt.next().split("\t").tail.toIndexedSeq
    require(countIt.forall(_.next().split("\t").tail sameElements samples),
            "Samples in count file are not the same as the distance matrix")
    val sampleMap = samples.zipWithIndex.toMap

    val data = (for ((line, idx) <- readerIt.zipWithIndex) yield {
      val values = line.split("\t")
      val countValues = countIt.map(_.next().split("\t"))
      require(sampleMap(values.head) == idx,
              "Order of rows is different then columns")
      require(countValues.forall(x => sampleMap(x.head) == idx),
              "Order of rows is different then columns in count file")
      values.tail.zipWithIndex.map {
        case (x, idx2) =>
          if (x == ".") None
          else {
            val distance = x.toDouble
            Some(
              countValues
                .map(_.tail(idx2).toInt)
                .map(distance / _)
                .getOrElse(distance))
          }
      }.toIndexedSeq
    }).toIndexedSeq
    reader.close()
    countReader.foreach(_.close())
    DistanceMatrix(data, samples)
  }

  def fromFileSpark(file: File, countFile: Option[File] = None)(
      implicit sc: SparkContext): DistanceMatrix = {

    val distanceLines =
      sc.textFile(file.getAbsolutePath, 200).map(_.split("\t"))
    val samples = distanceLines.first().tail
    val distances = distanceLines
      .zipWithIndex()
      .filter { case (_, idx) => idx != 0L }
      .flatMap {
        case (values, s1) =>
          values.tail.zipWithIndex
            .flatMap {
              case (v, s2) =>
                if (v == ".") None
                else Some((s1.toInt - 1, s2) -> v.toDouble)
            }
      }
    val dist = countFile match {
      case Some(cFile) =>
        val countLines = sc
          .textFile(cFile.getAbsolutePath, 200)
          .map(_.split("\t"))
        require(countLines.first().tail sameElements samples,
                "Samples in count file are not the same as the distance matrix")
        val counts = countLines
          .zipWithIndex()
          .filter { case (_, idx) => idx != 0L }
          .flatMap {
            case (values, s1) =>
              values.tail.zipWithIndex
                .flatMap {
                  case (v, s2) =>
                    if (v == ".") None
                    else Some((s1.toInt - 1, s2) -> v.toInt)
                }
          }
        distances.join(counts).map { case (k, (d, c)) => k -> (d / c) }
      case _ => distances
    }
    rddToMatrix(dist, samples)
  }

  def rddToMatrix(rdd: RDD[((Int, Int), Double)],
                  samples: IndexedSeq[String]): DistanceMatrix = {
    rdd
      .groupBy { case ((x, _), _) => x }
      .map {
        case (rowId, row) =>
          val map = row.map { case ((_, columnId), value) => columnId -> value }.toMap
          rowId -> samples.indices.map(map.get)
      }
      .repartition(1)
      .mapPartitions { it =>
        val map = it.toMap
        Iterator(
          DistanceMatrix(samples.indices
                           .map(
                             map
                               .getOrElse(_,
                                          IndexedSeq.fill[Option[Double]](
                                            samples.length)(None))),
                         samples))
      }
      .first()

  }
}
