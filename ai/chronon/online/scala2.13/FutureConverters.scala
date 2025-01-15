object FutureConverters{
        def toJava[T](f:Future[T]):CompletionStage[T]={
        (new JFutConv.FutureOps(f)).asJava
        }

        def toScala[T](cs:CompletionStage[T]):Future[T]={
        (new JFutConv.CompletionStageOps(cs)).asScala
        }
        }
