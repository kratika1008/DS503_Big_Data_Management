import java.io.FileWriter

object CreatePoints {

  def main(args: Array[String]): Unit = {
      val minVal: Int = 1
      val maxVal: Int = 10000
      val txtWriter: FileWriter = new FileWriter("Points.txt")
      for (i <- 0.until(11000000)) {
        val x: Int = scala.util.Random.nextInt(maxVal - minVal + 1) + minVal
        val y: Int = scala.util.Random.nextInt(maxVal - minVal + 1) + minVal
        txtWriter.write(String.valueOf(x) + "," + String.valueOf(y) + "\n")
      }
      txtWriter.flush()
      txtWriter.close()
  }

}

