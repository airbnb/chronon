import org.slf4j.LoggerFactory
import java.io.File
import scala.reflect.io.Directory

object Folder {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  def clean(files: File*): Unit = {
    logger.info(s"Removing folders ${files.map(_.getAbsolutePath)}")
    files.foreach { file =>
      if (file.exists() && file.isDirectory) {
        val directory = new Directory(file)
        directory.deleteRecursively()
      }
    }
  }
}
