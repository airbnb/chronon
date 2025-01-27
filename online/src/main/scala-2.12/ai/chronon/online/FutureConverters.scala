package ai.chronon.online

import java.util.concurrent.CompletionStage
import scala.concurrent.Future
import scala.compat.java8.{FutureConverters => JFutConv}

object FutureConverters {
  def toJava[T](f: Future[T]): CompletionStage[T] = {
    JFutConv.toJava(f)
  }

  def toScala[T](cs: CompletionStage[T]): Future[T] = {
    JFutConv.toScala(cs)
  }
}
