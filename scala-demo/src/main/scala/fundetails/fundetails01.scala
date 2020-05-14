package fundetails

object fundetails {
  def main(args: Array[String]): Unit = {
    println(test)

    val tiger = new Tiger
    val tiger1: Tiger = test01(10, tiger)
    println(tiger1.name)
    println(tiger.name)
  }

  def test: Int = {
     1
  }

  def test01(n1: Int, tiger: Tiger): Tiger = {
    tiger.name = "jack"
    tiger
  }
}

class Tiger {

  var name = ""
}