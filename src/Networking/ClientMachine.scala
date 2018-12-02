package Networking

import java.io.BufferedReader
import java.io.PrintWriter
import java.net.Socket
import java.io.InputStreamReader

object ClientMachine {
  def main(args:Array[String]) {
    
    
    val soc = new Socket("localhost",4444)
         
    val inputReader = new BufferedReader(new InputStreamReader(soc.getInputStream))
    val postOutput = new PrintWriter(soc.getOutputStream)
    
    while (true){
    val input = readLine
    postOutput.append(input)
    
    }
}
}