package ai.chronon.online

import java.util.concurrent.CompletionStage
import scala.concurrent.Future
import scala.jdk.{FutureConverters => JFutConv}

object FutureConverters {
  def toJava[T](f: Future[T]): CompletionStage[T] = {
    (new JFutConv.FutureOps(f)).asJava
  }

  def toScala[T](cs: CompletionStage[T]): Future[T] = {
    (new JFutConv.CompletionStageOps(cs)).asScala
  }
}
