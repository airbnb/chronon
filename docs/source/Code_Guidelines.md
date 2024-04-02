# Code Guidelines

## Scala Language usage Philosophy

There are two phases of execution in chronon code. Hot path & control path.
Hot path is in sense the "inner loop" of the larger program. The speed of the the inner
loop controls the speed of the whole program. While changes to control path are less impactful.
Any row/column level operation is a part of the inner loop. This is true across all online & offline workflows.

### Rules for inner loops

- Don't use scala `for` loops
- Don't use ranges : `foreach`, `until`, `to`
- Don't use `Option`s and `Tuple`s
- Don't use immutable collections
- Create naked Arrays when the size is known ahead of time
- Move as many branches as possible out of hot path into the control path.
    - Leverage data schemas to do so.
        - Example: Convert arbitrary numeric value of a `df` at position `columnIndex`
          Branches in hot path (two `isInstance` calls per value)
          ```scala
          // inefficient version
          
          def toDouble(o: Any): Double = {
              if (o.isInstance[Int]) { 
                o.asInstanceOf[Int].toDouble
              } else if (o.isInstance[Long]) { 
                o.asInstanceOf[Long].toDouble
              } 
              . . .
          }
          
          df.rdd.map(toDouble(row(columnIndex)))
          ``` 

          Branches in control path (one `isInstance` call per value)
          ```scala
          // efficient version
          
          def toDoubleFunc(inputType: DataType): Any => Double = {
              inputType match { 
                case IntType => x: Any => x.asInstanceOf[Int].toDouble 
                case LongType => x: Any => x.asInstanceOf[Long].toDouble 
              } 
          }
          val doubleFunc = toDoubleFunc(df.schema(columnIndex).dataType)
          df.rdd.map(doubleFunc(row(columnIndex)))
          ```

### Readability

Scala is a large language with a lot of powerful features.
Some of these features however were added without regard to readability.
The biggest culprit is the overloading of the 
[implicit](https://www.scala-lang.org/blog/2020/05/05/scala-3-import-suggestions.html)
keyword.

We have restricted the code base to use implicit only to retroactively extend 
classes. A.K.A as extension objects. Every other use should be minimized.

Scala 3 fixes a lot of these design mistakes, but the world is quite far from 
adopting Scala 3.

Having said all that, Scala 2 is leagues ahead of any other language on JVM, 
in terms of power. Also Spark APIs are mainly in Scala2.  

### Testing

Every new behavior should be unit-tested. We have implemented a fuzzing framework 
that can produce data randomly as scala objects or 
spark tables - [see](../../spark/src/test/scala/ai/chronon/spark/test/DataFrameGen.scala). Use it for testing.
Python code is also covered by tests - [see](https://github.com/airbnb/chronon/tree/main/api/py/test).