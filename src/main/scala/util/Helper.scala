package util

import java.io.File
import java.nio.file.Paths


object Helper {

  //copied from https://alvinalexander.com/scala/how-to-list-files-in-directory-filter-names-scala/
  // for time box purpose
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def stripPath(path_name: String):String= {

    Paths.get(path_name).getFileName.toString
  }

  def getFileExtension(file_name: String):String= {
    file_name.split('.')(1)
  }
  def getSourceName(file_name: String):String= {
    file_name.split('.')(0)
  }


  def mapNameToFiletype( file: String) = {
    val list = file.split('.').toList

    Map("source" -> list(0).toString, "filetype" -> list(1).toString)
  }
}
