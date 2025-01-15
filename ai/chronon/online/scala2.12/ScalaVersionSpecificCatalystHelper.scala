package ai.chronon.online;object ScalaVersionSpecificCatalystHelper {

        def evalFilterExec(row: InternalRow, condition: Expression, attributes: Seq[Attribute]): Boolean = {
                val predicate = Predicate.create(condition, attributes)
                predicate.initialize(0)
                val r = predicate.eval(row)
                r
                }
        }
