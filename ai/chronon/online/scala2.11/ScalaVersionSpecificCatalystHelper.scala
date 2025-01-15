object ScalaVersionSpecificCatalystHelper{

        def evalFilterExec(row:InternalRow,condition:Expression,attributes:Seq[Attribute]):Boolean={
        val predicate=InterpretedPredicate.create(condition,attributes)
        predicate.initialize(0)
        val r=predicate.eval(row)
        r
        }
        }
