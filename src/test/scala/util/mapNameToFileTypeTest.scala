package util

import org.scalatest.funsuite.AnyFunSuite
import Helper.mapNameToFiletype

class mapNameToFileTypeTest extends AnyFunSuite{
  test("helper mapNameToFileType") {
    val source = List("schools.json",
      "school_lists.csv",
      "school_list_items.csv",
      "users.csv")

    val actual = source.map(Helper.mapNameToFiletype(_))
    println(actual)
    val expected = List(
      Map("source"->"schools","filetype" -> "json"),
      Map("source"->"school_lists","filetype" -> "csv"),
      Map("source"->"school_list_items","filetype" -> "csv"),
      Map("source"->"users","filetype" -> "csv")
    )
    assert(actual === expected)

  }
}
