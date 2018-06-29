package com.umeng.dp.yuliang.play

import swing._
import event._

/**
  * Created by gnailuy on 11/17/16.
  */
object TempConverter extends SimpleSwingApplication {
  def top = new MainFrame {
    title = "Celsius/Fahrenheit Converter"
    object celsius extends TextField { columns = 5 }
    object fahrenheit extends TextField { columns = 5 }
    contents = new FlowPanel {
      contents += celsius
      contents += new Label(" Celsius = ")
      contents += fahrenheit
      contents += new Label(" Fahrenheit = ")
      border = Swing.EmptyBorder(15, 10, 10, 10)
    }
    listenTo(celsius, fahrenheit)
    reactions += {
      case EditDone(`fahrenheit`) =>
        val f = fahrenheit.text.toDouble
        val c = (f - 32) * 5 / 9
        celsius.text = "%.2f".format(c)
      case EditDone(`celsius`) =>
        val c = celsius.text.toDouble
        val f = c * 9 / 5 + 32
        fahrenheit.text = "%.2f".format(f)
    }
  }
}

