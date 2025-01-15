object FutureConverters{
        def toJava[T](f:Future[T]):CompletionStage[T]={
        JFutConv.toJava(f)
        }

        def toScala[T](cs:CompletionStage[T]):Future[T]={
        JFutConv.toScala(cs)
        }
        }
