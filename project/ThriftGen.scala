import org.slf4j.LoggerFactory
import sbt.*

import scala.language.postfixOps
import sys.process.*

object Thrift {

  def print_and_execute(command: String): Unit = {
    println(s"+ $command")
    command !
  }

  def gen(inputPath: String, outputPath: String, language: String, cleanupSuffixPath: String = "", extension: String = null): Seq[File] = {
    s"""echo "Generating files from thrift file: $inputPath \ninto folder $outputPath" """ !;
    print_and_execute(s"rm -rf $outputPath/$cleanupSuffixPath")
    s"mkdir -p $outputPath" !;
    print_and_execute(s"thrift -version")
    print_and_execute(s"thrift --gen $language -out $outputPath $inputPath")
    val files = (PathFinder(new File(outputPath)) ** s"*.${Option(extension).getOrElse(language)}").get()
    println("Generated files list")
    files.map(_.getPath).foreach { path => println(s"    $path") }
    println("\n")
    files
  }
}
