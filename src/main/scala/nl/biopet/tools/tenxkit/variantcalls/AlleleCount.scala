package nl.biopet.tools.tenxkit.variantcalls

case class AlleleCount(forwardUmi: Int,
                       reverseUmi: Int,
                       forwardReads: Int,
                       reverseReads: Int) {
  def total: Int = forwardUmi + reverseUmi
  def totalReads: Int = forwardReads + reverseReads

  def +(other: AlleleCount): AlleleCount = {
    AlleleCount(this.forwardUmi + other.forwardUmi,
                this.reverseUmi + other.reverseUmi,
                this.forwardReads + other.forwardReads,
                this.reverseReads + other.reverseReads)
  }
}

object AlleleCount {
  def apply(forward: Int = 0, reverse: Int = 0): AlleleCount = {
    AlleleCount(forward, reverse, forward, reverse)
  }
}
