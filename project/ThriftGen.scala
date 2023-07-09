import sbt._

import sys.process._

object Thrift {
  def gen(inputPath: String, outputPath: String, language: String, insideCI: Boolean, insideDockerLocal: Boolean,  cleanupSuffixPath: String = "", extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift file: $inputPath \ninto folder $outputPath; This build is running in CI: $insideCI; This build is running using the Docker Local Image: $insideDockerLocal;" """ !;
    s"rm -rf $outputPath/$cleanupSuffixPath" !;
    s"mkdir -p $outputPath" !;
    if (insideCI || insideDockerLocal) {
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
