package nl.biopet.tools.tenxkit.variantcalls

case class AlleleCount(forward: Int = 0,
                       reverse: Int = 0) {
  def total: Int = forward + reverse
}
