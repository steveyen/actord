package ff.actord

object Util {
  final val ZERO      = java.lang.Integer.valueOf(0)
  final val CR        = '\r'.asInstanceOf[Byte]
  final val CRNL      = "\r\n"
  final val CRNLBytes = CRNL.getBytes

  def nowInSeconds: Long = System.currentTimeMillis / 1000
}

