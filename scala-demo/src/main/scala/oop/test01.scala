package oop

object test01 {
  def main(args: Array[String]): Unit = {
    val cat = new Cat
    cat.name = "小白"
    cat.age = 18
    cat.color = "白色"
  }
}

class Cat() {
  // 当我们声明了var name:String 在底层对应 private String name
  // 会同时生成两个public方法 name() getter，public name——$eq(name:String) setter
  var name: String = _
  var age: Int = _
  var color: String = _
}
