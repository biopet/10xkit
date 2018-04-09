package nl.biopet.tools.tenxkit

import java.io.{File, PrintWriter}

import nl.biopet.utils.Logging

import scala.io.Source

case class DistanceMatrix(values: Array[Array[Option[Double]]],
                          samples: Array[String])
    extends Logging {

  def overlapSamples(sample: Int, cutoff: Double): Array[String] = {
    samples.zipWithIndex
      .filter {
        case (name, idx) =>
          val v1 = values(sample)(idx)
          val v2 = values(idx)(sample)
          (v1, v2) match {
            case (Some(v), _) => v <= cutoff
            case (_, Some(v)) => v <= cutoff
            case _            => false
          }
      }
      .map(_._1)
  }

  def extractSamples(extractSamples: List[String]): DistanceMatrix = {
    val newSamples =
      samples.zipWithIndex.filter(x => extractSamples.contains(x._1))
    val idxs = newSamples.map(_._2)
    val newValues = for (s1 <- idxs) yield {
      for (s2 <- idxs) yield {
        values(s1)(s2)
      }
    }
    DistanceMatrix(newValues, newSamples.map(_._1))
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

  def subgroupHistograms(name: String,
                         barcodes: List[String]): SubgroupHistogram = {
    logger.info(s"Making histograms for '$name'")

    val barcodesIdx = barcodes.map(samples.indexOf).toSet
    val sampleHistogram = new DistanceHistogram
    val notSampleHistogram = new DistanceHistogram

    for {
      (list, s1) <- values.zipWithIndex
      (value, s2) <- list.zipWithIndex
      c1 = barcodesIdx.contains(s1)
      c2 = barcodesIdx.contains(s2)
      if c1 || c2
    } {
      if (c1 && c2) value.foreach(sampleHistogram.add)
      else value.foreach(notSampleHistogram.add)
    }
    SubgroupHistogram(sampleHistogram, notSampleHistogram)
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
