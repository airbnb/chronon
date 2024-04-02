package ai.chronon.flink.window

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Custom Flink Trigger that fires on every event received.
  * */
class AlwaysFireOnElementTrigger extends Trigger[Map[String, Any], TimeWindow] {
  override def onElement(
      element: Map[String, Any],
      timestamp: Long,
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): TriggerResult =
    TriggerResult.FIRE

  override def onProcessingTime(
      time: Long,
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): TriggerResult =
    TriggerResult.CONTINUE

  override def onEventTime(
      time: Long,
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): TriggerResult =
    // We don't need to PURGE here since we don't have explicit state.
    // Flink's "Window Lifecycle" doc: "The window is completely removed when the time (event or processing time)
    // passes its end timestamp plus the user-specified allowed lateness"
    TriggerResult.CONTINUE

  // This Trigger doesn't hold state, so we don't need to do anything when the window is purged.
  override def clear(
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): Unit = {}

  override def canMerge: Boolean = true

  override def onMerge(
      window: TimeWindow,
      mergeContext: Trigger.OnMergeContext
  ): Unit = {}
}

/**
  * BufferedProcessingTimeTrigger is a custom Trigger that fires at most every 'bufferSizeMillis' within a window.
  * It is intended for incremental window aggregations using event-time semantics.
  *
  * Purpose: This trigger exists as an optimization to reduce the number of writes to our online store and better handle
  * contention that arises from having hot keys.
  *
  * Details:
  *  - The buffer timers are NOT aligned with the UNIX Epoch, they can fire at any timestamp. e.g., if the first
  *  event arrives at 14ms, and the buffer size is 100ms, the timer will fire at 114ms.
  *  - Buffer timers are only scheduled when events come in. If there's a gap in events, this trigger won't fire.
  *
  * Edge cases handled:
  * - If the (event-time) window closes before the last (processing-time) buffer fires, this trigger will fire
  * the remaining buffered elements before closing.
  *
  * Example:
  *    Window size = 300,000 ms (5 minutes)
  *    BufferSizeMillis = 100 ms.
  *    Assume we are using this trigger on a GroupBy that counts the number unique IDs see.
  *    For simplicity, assume event time and processing time are synchronized (although in practice this is never true)
  *
  *    Event 1: ts = 14 ms, ID = A.
  *        preAggregate (a Set that keeps track of all unique IDs seen) = [A]
  *        this causes a timer to be set for timestamp = 114 ms.
  *    Event 2: ts = 38 ms, ID = B.
  *        preAggregate = [A, B]
  *    Event 3: ts = 77 ms, ID = B.
  *        preAggregate = [A, B]
  *    Timer set for 114ms fires.
  *        we emit the preAggregate [A, B].
  *    Event 4: ts = 400ms, ID = C.
  *        preAggregate = [A,B,C] (we don't purge the previous events when the time fires!)
  *        this causes a timer to be set for timestamp = 500 ms
  *    Timer set for 500ms fires.
  *        we emit the preAggregate [A, B, C].
  * */
class BufferedProcessingTimeTrigger(bufferSizeMillis: Long) extends Trigger[Map[String, Any], TimeWindow] {
  // Each pane has its own state. A Flink pane is an actual instance of a defined window for a given key.
  private val nextTimerTimestampStateDescriptor =
    new ValueStateDescriptor[java.lang.Long]("nextTimerTimestampState", classOf[java.lang.Long])

  /**
    * When an element arrives, set up a processing time trigger to fire after `bufferSizeMillis`.
    * If a timer is already set, we don't want to create a new one.
    *
    * Late events are treated the same way as regular events; they will still get buffered.
    */
  override def onElement(
      element: Map[String, Any],
      timestamp: Long,
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): TriggerResult = {
    val nextTimerTimestampState: ValueState[java.lang.Long] = ctx.getPartitionedState(
      nextTimerTimestampStateDescriptor
    )

    // Set timer if one doesn't already exist
    if (nextTimerTimestampState.value() == null) {
      val nextFireTimestampMillis = ctx.getCurrentProcessingTime + bufferSizeMillis
      ctx.registerProcessingTimeTimer(nextFireTimestampMillis)
      nextTimerTimestampState.update(nextFireTimestampMillis)
    }

    TriggerResult.CONTINUE
  }

  /**
    * When the processing-time timer set up in `onElement` fires, we emit the results without purging the window.
    * i.e., we keep the current pre-aggregates/IRs in the window so we can continue aggregating.
    *
    * Note: We don't need to PURGE the window anywhere. Flink will do that automatically when a window expires.
    * Flink Docs: "[...] Flink keeps the state of windows until their allowed lateness expires. Once this happens, Flink
    * removes the window and deletes its state [...]".
    *
    * Note: In case the app crashes after a processing-time timer is set, but before it fires, it will fire immediately
    * after recovery.
    */
  override def onProcessingTime(
      timestamp: Long,
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): TriggerResult = {
    val nextTimerTimestampState = ctx.getPartitionedState(nextTimerTimestampStateDescriptor)
    nextTimerTimestampState.update(null)
    TriggerResult.FIRE
  }

  /**
    * Fire any elements left in the buffer if the window ends before the last processing-time timer is fired.
    * This can happen because we are using event-time semantics for the window, and processing-time for the buffer timer.
    *
    * Flink automatically sets up an event timer for the end of the window (+ allowed lateness) as soon as it
    * sees the first element in it. See 'registerCleanupTimer' in Flink's 'WindowOperator.java'.
    */
  override def onEventTime(
      timestamp: Long,
      window: TimeWindow,
      ctx: Trigger.TriggerContext
  ): TriggerResult = {
    val nextTimerTimestampState: ValueState[java.lang.Long] = ctx.getPartitionedState(
      nextTimerTimestampStateDescriptor
    )
    if (nextTimerTimestampState.value() != null) {
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  /**
    * When a window is being purged (e.g., because it has expired), we delete timers and state.
    *
    * This function is called immediately after our 'onEventTime' which fires at the end of the window.
    * See 'onEventTime' in Flink's 'WindowOperator.java'.
    */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // Remove the lingering processing-time timer if it exist.
    val nextTimerTimestampState: ValueState[java.lang.Long] = ctx.getPartitionedState(
      nextTimerTimestampStateDescriptor
    )
    val nextTimerTimestampStateValue = nextTimerTimestampState.value()
    if (nextTimerTimestampStateValue != null) {
      ctx.deleteProcessingTimeTimer(nextTimerTimestampStateValue)
    }

    // Delete state
    nextTimerTimestampState.clear()
  }
}
