package org.minyodev.examples

/**
  * Recursive tree traversals
  * Given
  *     A
  *    / \
  *   B   C
  *  / \
  * D  E
  *
  * Inorder: D, B, E, A, C
  * Preorder: A, B, D, E, C
  * Postorder: D, E, B, C, A
  */
object BinaryTree extends App {

  trait Tree
  case class Node(value: String, left: Tree, right: Tree) extends Tree
  case class Leaf(s: String) extends Tree

  val tree = Node("A", Node("B", Leaf("D"), Leaf("E")), Leaf("C"))
  val list = List(1,List(2, List(4, 5), 3))

  println("\nInorder")
  inorder(tree)
  println("\nPreorder")
  preorder(tree)
  println("\nPostorder")
  postorder(tree)
  println("\nBFS")
  bfs(list)

  def inorder(t: Tree): Unit = {
    t match {
      case Leaf(v) => print(v + " ")
      case Node(v, left, right) => {
        inorder(left)
        print(v + " ")
        inorder(right)
      }
    }
  }

  def preorder(t: Tree): Unit = {
    t match {
      case Node(v, left, right) => {
        print(v + " ")
        preorder(left)
        preorder(right)
      }
      case Leaf(v) =>
        print(v + " ")
    }
  }

  def postorder(t: Tree): Unit = {
    t match {
      case Leaf(v) => print(v + " ")
      case Node(v, left, right) => {
        postorder(left)
        postorder(right)
        print(v + " ")
      }
    }
  }

  def bfs(l: List[Any]) {
    val allElements: Boolean = l.foldLeft(true)((a, c) =>
      c match {
        case n: Int => a & true
        case ln: List[Any] => a & false
      })

    l.filter(_ match {
      case n: Int => true case _ => false
    })
      .foreach(m => print(m + " "))

    if(!allElements) {
      bfs(l.filter(_ match {
        case ln: List[Any] => true case _ => false
      })
        .flatMap(l => l.asInstanceOf[List[Any]]))
    }
  }
}
