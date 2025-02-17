package ai.chronon.online.test

// A couple of toy UDFs to help test Hive UDF registration in CatalystUtil
class Minus_One extends org.apache.hadoop.hive.ql.exec.UDF {
  def evaluate(x: Integer): Integer = {
    x - 1
  }
}

class Cat_Str extends org.apache.hadoop.hive.ql.exec.UDF {
  def evaluate(x: String): String = {
    x + "123"
  }
}
