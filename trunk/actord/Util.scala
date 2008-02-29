package ff.actord

object Util {
  final val ZERO      = java.lang.Integer.valueOf(0)
  final val CR        = '\r'.asInstanceOf[Byte]
  final val CRNL      = "\r\n"
  final val CRNLBytes = CRNL.getBytes

  def nowInSeconds: Long = System.currentTimeMillis / 1000
  
  def itemToLong(items: Array[String], at: Int) =
    if (items.length > at)
      parseLong(items(at), 0L)
    else 
      0L
      
  def parseLong(s: String, defaultVal: Long) = try { 
    items(at).toLong 
  } catch { 
    case _ => defaultVal
  }
}

