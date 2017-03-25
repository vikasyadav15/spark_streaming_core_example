package vikas.test

/**
  * Created by vikasyadav on 1/2/17.
  */
import scala.io.Source
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter


object FileReader {

  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("/Users/vikasyadav/Music/data/tes")
    val outputFile = new File("/Users/vikasyadav/Music/data/out.txt")
    val writer = new BufferedWriter(new FileWriter(outputFile))
    // use curly brackets {} to tell Scala that it's now a multi-line statement!
    file.getLines().foreach{ line =>
      println(line)
      writer.write("***" + line + "***")
      writer.newLine()
    }
    writer.flush()
    writer.close()
  }

}