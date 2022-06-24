import java.io.File
import scala.reflect.io.Directory

object Folder {
  def clean(files: File*): Unit = {
    println(s"Removing folders ${files.map(_.getAbsolutePath)}")
    files.foreach { file =>
      if (file.exists() && file.isDirectory) {
        val directory = new Directory(file)
        directory.deleteRecursively()
      }
    }
  }
}
