package reductions

import common._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var left, right = 0
    for (c <- chars) {
      c match {
        case '(' => left += 1
        case ')' =>
          if (left > 0) left -= 1
          else right += 1
        case _ => // do nothing
      }
    }
    (left, right) == (0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int): (Int, Int) = {
      var i = idx
      var left, right = 0
      while (i < until) { // while loop works almost twice faster, than for (i <- idx until until) :-)
        if (chars(i) == '(')
          left += 1
        else if (chars(i) == ')') {
          if (left > 0)
            left -= 1
          else
            right += 1
        }
        i += 1
      }
      (left, right)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold)
        traverse(from, until)
      else {
        val mid = from + (until - from) / 2
        val ((l1, r1), (l2, r2)) = parallel(reduce(from, mid), reduce(mid, until))
        (Math.max(0, l1 - r2) + l2, Math.max(0, r2 - l1) + r1)
      }
    }

    reduce(0, chars.length) == (0 , 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
