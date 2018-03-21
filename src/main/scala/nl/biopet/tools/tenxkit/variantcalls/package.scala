package nl.biopet.tools.tenxkit

package object variantcalls {
  case class SampleRead(sample: String,
                        contig: String,
                        start: Long,
                        sequence: String,
                        quality: String,
                        cigar: String)
}
