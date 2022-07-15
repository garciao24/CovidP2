package P2

object userMenu {
  private var bool : Boolean = false
  private var currentUser = ""

  def CreateNewAccount(): Unit = {
    println("Create an account to access database")
    println("Please enter in your first name")
    val firstName = scala.io.StdIn.readLine()
    println("Please enter in your last name")
    val lastName = scala.io.StdIn.readLine()

    do {
      println("Please enter in your username")
      currentUser = scala.io.StdIn.readLine()
      bool = Login.checkifExists(currentUser)
    }while(bool)

    println("Please enter in your password")
    val usersPassword = scala.io.StdIn.readLine()
    val adminAuth = 0

    val newAccount = Login.createUser(firstName, lastName, currentUser, usersPassword, adminAuth)
    if (newAccount) {
      println("You have successfully created an account")
      ////mainmenu function
    } else
    CreateNewAccount()
  }





}
