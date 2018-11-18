package com.github.propi.dbmaps.utils

import java.io.File

import scala.language.{postfixOps, reflectiveCalls}

/**
  * Created by Vaclav Zeman on 26. 9. 2018.
  */
object FileOps {

  def getFileOrCreate(path: String): File = getFileOrCreate(path, false)

  def getFileOrCreate(path: String, rewrite: Boolean): File = getFileOrCreate(new File(path), rewrite)

  def getFileOrCreate(file: File): File = getFileOrCreate(file, false)

  def getFileOrCreate(file: File, rewrite: Boolean): File = {
    getDirOrCreate(file.getParentFile)
    if (rewrite && file.isFile) {
      file.delete()
    }
    file.createNewFile()
    file
  }

  def getDirOrCreate(file: File): File = {
    if (!file.isDirectory) file.mkdirs()
    file
  }

  def getDirOrCreate(parent: File, path: String): File = getDirOrCreate(new File(parent, path))

  def getDirOrCreate(path: String): File = getDirOrCreate(new File(path))

}