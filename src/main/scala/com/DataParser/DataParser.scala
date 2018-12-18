package com.DataParser

import scala.io.Source
import java.io.File

import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}


object DataParser {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Una vez descargado el JSON hay que interpretarlo.
  val conf = new SparkConf().setAppName("DataParser").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)



  //Método de obtención de Ficheros
  def obtenerListaFicheros(dir: String):List[File] = {
    val directorio = new File(dir)

    //Añadir más extensiones si fuera necesario
    val Extension = List("json")
    if (directorio.exists() && directorio.isDirectory) {
      //Obtenemos lista de ficheros con extensión .json
      directorio.listFiles(_.isFile).toList.filter { file => Extension.exists(file.getName.endsWith(_))}
    } else {
      //Lo devolvemos vacío
      List[File]()
    }
  }

  def main(args: Array[String]) {

    val listaFicheros = obtenerListaFicheros("Data/")

    for (n <- listaFicheros) {
      println("El directorio out/ : " + n.toString)
    }

    val fichero = listaFicheros.maxBy(_.lastModified())

    println("El fichero más reciente: " + fichero.toString)
    if (fichero.isFile) {
      println(" ")
      println("El fichero existe.")
      println(" ")

      implicit val formats = DefaultFormats

      case class Subasta(auc: Array[Int], owner: Array[String], ownerRealm: Array[String], bid: Array[BigInt],
                         buyout: Array[BigInt], quantity: Array[Int], timeLeft: Array[String], rand: Array[Int], seed: Array[Int], context: Array[Int])

      val jValues = parse(Source.fromFile(fichero.toString).mkString)

      val lista_subastas = (jValues \\ "auctions").children

      for ( list <- lista_subastas ) {

        val datos = list.extract[Subasta]
        println(datos.owner(0).toString)

        //for ( nombres <- datos.owner) println(nombres)
        //Apuesta mínima
        println(s"Apuesta mínima: ${datos.bid.reduce(_ min _)}")
        println(s"Apuesta máxima: ${datos.bid.reduce(_ max _)}")

        val rdd_precios = sc.parallelize(List(datos.auc,datos.owner,datos.buyout))


        //rdd_precios.collect.foreach(println)
        

        case class RDD_NUEVO(id: BigInt,owner: String,buyout:BigInt)
        //rdd_precios.map{ case (k,v) => k -> v.toList.sortBy(_.owner)}


      }

    } else {
      println("El fichero NO existe.")
    }

  }
}
