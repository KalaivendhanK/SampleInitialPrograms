package Networking

import java.net._
import java.io._


object LocalServer {
  def main(args:Array[String]) {
    
    
    val servSoc = new ServerSocket(4444)
    
    val openSocket = servSoc.accept
    val inputReader = new BufferedReader(new InputStreamReader(openSocket.getInputStream))
    val postOutput = new PrintWriter(openSocket.getOutputStream)
    postOutput.append("Hey there !!!")
    //val textFromUser = inputReader.readLine()
    //postOutput.append(s"You said $textFromUser")
    
    //postOutput.append(readLine())
    println(inputReader.readLine)
    
  }
}