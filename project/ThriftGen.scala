import sbt._

import sys.process._

object Thrift {
  def gen(inputPath: String, outputPath: String, language: String, extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift file: $outputPath \ninto folder $inputPath" """ !;
    s"rm -rf $outputPath" !;
    s"mkdir -p $outputPath" !;
    s"thrift --gen $language -out $outputPath $inputPath" !;
    val files = (PathFinder(new File(outputPath)) ** s"*.${Option(extension).getOrElse(language)}").get()
    println("Generated files list")
    files.map(_.getPath).foreach { path => println(s"    $path") }
    println("\n")
    files
  }
}
