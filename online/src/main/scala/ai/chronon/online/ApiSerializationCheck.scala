/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.{Field, Method, Modifier}
import scala.util.Try

/**
  * Verifies that an [[Api]] implementation survives the driver -> executor hop.
  *
  * Streaming jobs Java-serialize the `Api` into Spark task closures (the KV write and the
  * write-notification publish run inside `mapPartitions`). A non-lazy `@transient val` is
  * initialized by the constructor on the driver and skipped by serialization, so it is null on
  * every executor. Any code path that touches it there throws a NullPointerException, and if the
  * field is the logger, the error handler itself throws and the failure is invisible.
  *
  * The check round-trips the object through Java serialization and reports every field that was
  * non-null before and is null afterwards, unless a same-named zero-arg accessor on the copy
  * returns a value (that is a `lazy val`, which re-initializes on first access and is safe).
  *
  * Implementers should call [[verify]] from a unit test against their own `Api`, for example
  * `ApiSerializationCheck.verify(new MyApi(conf))`, so a field that breaks on executors fails the
  * build instead of silently degrading in production.
  */
object ApiSerializationCheck {

  case class Violation(owner: String, field: String) {
    override def toString: String = s"$owner.$field"
  }

  class ApiNotExecutorSafeException(message: String) extends IllegalStateException(message)

  /** Java-serialization round trip, the same mechanism Spark uses for task closures. */
  def roundTrip[T <: AnyRef](obj: T): T = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    try out.writeObject(obj)
    finally out.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try in.readObject().asInstanceOf[T]
    finally in.close()
  }

  /**
    * Returns the fields of `original` that would be null on an executor.
    *
    * @param allowNullFields simple field names that are legitimately null after deserialization,
    *                        for example a `@transient var` that the implementation re-creates on
    *                        first use.
    */
  def findViolations(original: AnyRef, allowNullFields: Set[String] = Set.empty): Seq[Violation] = {
    val copy = roundTrip(original)
    classHierarchy(original.getClass)
      .flatMap { cls =>
        cls.getDeclaredFields.toSeq
          .filterNot(f => Modifier.isStatic(f.getModifiers))
          .filterNot(f => f.isSynthetic || f.getName.contains("bitmap$")) // lazy val init flags
          .filterNot(f => allowNullFields.contains(simpleName(f)))
          .filter { f =>
            f.setAccessible(true)
            val before = f.get(original)
            val after = f.get(copy)
            before != null && after == null && !reinitializesOnAccess(copy, cls, f)
          }
          .map(f => Violation(cls.getName, simpleName(f)))
      }
  }

  /** Throws [[ApiNotExecutorSafeException]] listing every offending field, otherwise no-op. */
  def verify(api: AnyRef, allowNullFields: Set[String] = Set.empty): Unit = {
    val violations = findViolations(api, allowNullFields)
    if (violations.nonEmpty) {
      throw new ApiNotExecutorSafeException(
        s"${api.getClass.getName} is not safe to serialize to Spark executors. " +
          s"These fields are initialized on the driver but null after deserialization: " +
          s"${violations.mkString(", ")}. Declare them as `@transient lazy val`, or re-create them " +
          s"on first use. See ApiSerializationCheck for details."
      )
    }
  }

  private def classHierarchy(cls: Class[_]): Seq[Class[_]] =
    Iterator.iterate[Class[_]](cls)(_.getSuperclass).takeWhile(c => c != null && c != classOf[Object]).toSeq

  // Scala may name-mangle private members accessed from inner classes: `a$b$C$$field`.
  private def simpleName(f: Field): String = f.getName.split("\\$\\$").last

  // A lazy val compiles to a null field plus an accessor that initializes it on first call. If
  // the copy exposes a zero-arg method with the field's name and it returns non-null, the field
  // heals itself on the executor and is not a violation.
  private def reinitializesOnAccess(copy: AnyRef, cls: Class[_], f: Field): Boolean = {
    val name = simpleName(f)
    val candidates: Seq[Method] = cls.getDeclaredMethods.toSeq
      .filter(m => m.getParameterCount == 0)
      .filter(m => m.getName == name || m.getName.endsWith("$$" + name))
    candidates.exists { m =>
      Try {
        m.setAccessible(true)
        m.invoke(copy) != null
      }.getOrElse(false)
    }
  }
}
