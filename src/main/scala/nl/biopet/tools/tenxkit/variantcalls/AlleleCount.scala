package nl.biopet.tools.tenxkit.variantcalls

case class AlleleCount(allele: String, forward: Int, reversed: Int, reference: Boolean) {
  def total: Int = forward + reversed
}
