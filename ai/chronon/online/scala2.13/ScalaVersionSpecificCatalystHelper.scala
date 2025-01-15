object ScalaVersionSpecificCatalystHelper{

        def evalFilterExec(row:InternalRow,condition:Expression,attributes:Seq[Attribute]):Boolean={
        val predicate=Predicate.create(condition,attributes.toSeq)
        predicate.initialize(0)
        val r=predicate.eval(row)
        r
        }
        }
