package com.DataOrigin

import java.io

import scala.reflect.io.Directory

object downloadJson {

  def fileDownloader(url: String, filename: String) = {
    try {

      println("Si existe " + filename + " lo borramos.")
      val fichero = new Directory(new io.File("out/"+filename))
      fichero.delete()

      val src = scala.io.Source.fromURL(url)
      val out = new java.io.FileWriter("out/"+filename)
      out.write(src.mkString)
      out.close()
    } catch {
      case e: java.io.IOException => "Algo malo ha pasado..."
    }
    //new URL(url) #> new File(filename) !!
  }

}
