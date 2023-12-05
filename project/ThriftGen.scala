import org.slf4j.LoggerFactory
import sbt._

import sys.process._

object Thrift {
  private val logger = LoggerFactory.getLogger(getClass)
  def gen(inputPath: String, outputPath: String, language: String, cleanupSuffixPath: String = "", extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift file: $outputPath \ninto folder $inputPath" """ !;
    s"rm -rf $outputPath/$cleanupSuffixPath" !;
    s"mkdir -p $outputPath" !;
    s"thrift --gen $language -out $outputPath $inputPath" !;
    val files = (PathFinder(new File(outputPath)) ** s"*.${Option(extension).getOrElse(language)}").get()
    logger.info("Generated files list")
    files.map(_.getPath).foreach { path => logger.info(s"    $path") }
    logger.info("\n")
    files
  }
}
