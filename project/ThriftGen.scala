import sbt._

import sys.process._

object Thrift {
  def gen(inputPath: String, outputPath: String, language: String, insideCI: Boolean, extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift file: $inputPath \ninto folder $outputPath; This build is running in CI: $insideCI" """ !;
    s"rm -rf $outputPath" !;
    s"mkdir -p $outputPath" !;
    if (insideCI) {
      s"thrift --gen $language -out $outputPath $inputPath" !
    };
    else {
      val currWorkDir: String = "pwd" !!
      val mountPath: String = s"$currWorkDir/:/chronon".filterNot(_.isWhitespace)
      s"docker run -v $mountPath --rm chronon_thriftgen:latest" !
    };
    val files = (PathFinder(new File(outputPath)) ** s"*.${Option(extension).getOrElse(language)}").get()
    println("Generated files list")
    files.map(_.getPath).foreach { path => println(s"    $path") }
    println("\n")
    files
  }
}
