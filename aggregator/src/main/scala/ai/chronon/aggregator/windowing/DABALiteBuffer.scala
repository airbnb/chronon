package ai.chronon.aggregator.windowing

import scala.collection.mutable

import ai.chronon.aggregator.base.SimpleAggregator

// Implementing the DABA-lite algorithm proposed in: https://arxiv.org/pdf/2009.13768v1.pdf
class DABALiteBuffer[Input, IR >: Null, Output >: Null](aggregator: SimpleAggregator[Input, IR, Output])
    extends InOrderAggregationBuffer[Input, IR, Output] {
  private val deque = new mutable.Queue[InOrderAggregationEntry[IR]]

  // The following 4 variables split the queue into 5 segments.
  // The front segment, the left segment, the right segment, the "A" segment and the back segment.
  //
  //   K
  //   J    K
  //   I    J    K
  //   H    I    J
  //   G    H    I
  //   F    G    H
  //   E    F    G
  //   D    E    F                              K
  //   C    D    E                              J    J
  //   B    C    D      E                       I    I    I
  //   A    B    C      D    E      F    G      H    H    H    H       L    M
  // <    front    >  <  left  >  < right >   <       "A"        >  <   back   >
  //                              <       aggRA: FGHIJK          >  <aggBack: LM>
  //
  // The data is arrange into 5 segments: front, left, right, "A", back.
  // The front segment holds aggregations of its elements to the end of "A" segment.
  // The left and "A" segment holds aggregation result of elements in their ranges.
  // The right & back segment hold individual elements.
  //
  // In the DABA algorithm, a few conditions hold:
  // 1. len(left segment) == len(right segment), or r - l == a - r.
  // 2. if the queue is not empty, len(front segment) - 1 == len(back segment), or l == len(q) - b
  private var l, r, a, b =
    0 // indexes of the first element of the left segment, right segment, "A" segment and the back segment

  private var aggBack: IR = null // aggregate result of the back segment
  private var aggRA: IR = null // aggregate result of the right segment and the "A" segment

  def peekBack(): InOrderAggregationEntry[IR] = if (deque.isEmpty) null else deque.head

  def pop(): InOrderAggregationEntry[IR] = {
    val head = deque.removeHead()
    // The first element is removed, shift all indexes to the left by 1
    l -= 1
    r -= 1
    a -= 1
    b -= 1
    fixup()

    head
  }

  def push(input: Input, ts: Long): Unit = {
    val ir = aggregator.prepare(input)
    deque.append(InOrderAggregationEntry(ir, ts))
    aggBack = if (aggBack == null) aggregator.clone(ir) else aggregator.merge(aggBack, ir)
    fixup()
  }

  def reset(): Unit = {
    deque.clear()
    l = 0
    r = 0
    a = 0
    b = 0
    aggBack = null
    aggRA = null
  }

  def query(): IR = {
    if (deque.isEmpty) {
      null
    } else if (aggBack == null) {
      aggregator.clone(deque.head.value)
    } else {
      aggregator.merge(aggregator.clone(deque.head.value), aggBack)
    }
  }

  private def fixup(): Unit = {
    // Whenever fixup is called, based on the l/r/a/b calculation, the length of the front segment
    // is the same as the back segment. The fixup function will address this to make sure:
    // len(front segment) - 1 == len(back segment). I.e., the front segment will always has one more
    // element than the back segment.
    if (b == 0) {
      // The queue has only one element, and it should be stored in the front segment.
      l = deque.length
      r = deque.length
      a = deque.length
      b = deque.length
      aggBack = null
      aggRA = null
    } else {
      if (l == b) {
        // Flip case:
        // The left segment, right segment & front segment are all emptied. The front segment and
        // the back segment contain same number of elements.
        // In this step, the front segment is converted to the right segment, and back segment is
        // converted to the "A" segment.
        l = 0
        a = deque.length
        b = deque.length
        aggRA = aggBack
        aggBack = null
      }

      if (l == r) {
        // Shift case:
        // There is no element in the left or right segment. Since "A" segment contains the
        // aggregation result of each of its elements up to the end of back segment, the first
        // element of the "A" segment is shifted to the end of the front segment.
        a += 1
        l += 1
        r += 1
      } else {
        // Shrink case:
        // There are same amount of elements in the left and the right segments.
        // The first elements of the left segment contains aggregation results of all elements in
        // the left segment. With this, the result of combining aggRA & the first element of left
        // will result in the aggregation result of all elements other than those in the front
        // segment. This result can be the new item of the front segment, and the left segment is
        // shrink by on from the left side.
        aggregator.merge(deque(l).value, aggRA)
        l += 1

        // The last element combined with the first element of the "A" segment will result in the
        // aggregation result of it and all elements in segment "A". Then we can extend "A" by 1 and
        // shrink the right segment by 1 from the right.
        if (a != b) {
          // Only when the "A" stack is not empty, there is a need to aggregate with right and "A"
          aggregator.merge(deque(a - 1).value, deque(a).value)
        }
        a -= 1
      }
    }
  }
}
