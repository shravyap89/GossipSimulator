import akka.actor.{Actor, ActorSystem, Props}
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration._

object Project2 {

  val push_sum_termination: Int = 3
  val gossip_termination_condition: Int = 10

  sealed trait gossip_simulation
  case class Start() extends gossip_simulation
  case class Gossip_converged() extends gossip_simulation
  case class Push_sum_converged() extends gossip_simulation
  case class push_sum_simulator(sum_from_source:Double, weight_from_source:Double)extends gossip_simulation
  case class gossip_simulator(start_point:String, rumor:String)extends gossip_simulation
  case class Gossip_worker_done(source:String) extends gossip_simulation
  case class Push_sum_worker_done(sum_estimate:Double)extends gossip_simulation
  case object Startgossip extends gossip_simulation
  case object Checktermninated extends gossip_simulation

  def main(args: Array[String]) {
    //println("No argugments found, please provide arguments")
    if (args.isEmpty) {
      println("ERROR: No input given, Please give input as <numNodes> <topology> <algorithm>" + "\n" + "Exiting the program")
      System.exit(1)
    }
    else if (args.length < 3) {
      println("ERROR: No of arguments less than 3, Please give input as <numNodes> <topology> <algorithm> " + "\n" + "Exiting the program")
      System.exit(1)
    }

    //Defined as val because argument will not change
    val numNodes = args(0).toInt

    val topology_arg: String = args(1).toString.toLowerCase()
    if (topology_arg != "full" && topology_arg != "3dgrid" && topology_arg != "line" && topology_arg != "im3dgrid") {
      println("ERROR: Argument given as topology is incorrect " + "\n" + "Exiting the program")
      System.exit(1)
    }

    val algorithm_arg: String = args(2).toString.toLowerCase()
    if (algorithm_arg != "gossip" && algorithm_arg != "pushsum") {
      println("ERROR: Argument given as algorithm is incorrect" + "\n" + "Exiting the program")
      System.exit(1)
    }

    println("Starting gossip simulation with : " + "\n" + "numNodes : " + numNodes + "\n" + "topology :" + topology_arg + "\n" + "algorithm : " + algorithm_arg + "\n")

    val system = ActorSystem("Proj2Simulation")
    val master = system.actorOf(Props(new Master(numNodes, topology_arg, algorithm_arg)), name = "master")

    master ! Start

  }

  class Master(num_nodes: Int, topology: String, algorithm: String) extends Actor {

    //set number of nodes based on the topology used
    var grid_size: Int = 0
    var node_count: Int = num_nodes
    var i: Int = 0
    var worker_name: String = null
    var start_point: String = null
    var k: Int = 0
    var rumor:String = "Rumor"
    var terminated_node = new ListBuffer[String]
    var terminated_node_count = 0
    var b:Long = 0

    var sum_weight_ratio: Double = 0
    //var weight_average:Double = 0

    override  def preStart() = {
      if (algorithm == "gossip") {
        //Interval set so that worker will start the scheduler
        val interval = 10
        import context.dispatcher
        context.system.scheduler.schedule(0 milliseconds, interval milliseconds, self, Checktermninated)
      }
    }

    def receive = {
      case Checktermninated =>
        if ( this.terminated_node_count == terminated_node_count){
          println("Convergence time = " + (System.currentTimeMillis()-b))
          context.stop(self)
          System.exit(0)
        }

      case Start =>
        //println("Master has sent the start message")
        if (topology == "3dgrid" || topology == "im3dgrid") {
          grid_size = math.cbrt(num_nodes.toDouble).toInt
          //println("grid size :"+ grid_size)
          while ((grid_size * grid_size * grid_size) != node_count) {
            node_count += 1
            grid_size = math.cbrt(node_count).toInt
          }
        }

        sum_weight_ratio = ((node_count.toDouble)*(node_count.toDouble -1)/2)/node_count.toDouble
        //println("Avergae ration is :"+ sum_weight_ratio)

        //println("grid size :"+ grid_size)
        //println("nodecount:" + node_count)

        for (i <- 0 to node_count-1) {
          context.actorOf(Props(new Worker(i, topology, algorithm, node_count, grid_size)), i.toString);
        }

        //println("Done assigning the workers properly")
        //Time calculation
        b = System.currentTimeMillis()
        if (algorithm == "gossip") {

          start_point = Random.nextInt(node_count).toString
          //println("Start point is :" + start_point)
          context.actorSelection(start_point) ! gossip_simulator(start_point, rumor)
        }
        else {
          start_point = Random.nextInt(node_count).toString
          //println("Start point is :" + start_point)
          context.actorSelection(start_point) ! push_sum_simulator(0,0)
        }


      case Gossip_worker_done(source:String) => {

        if (!(terminated_node contains source)){
          terminated_node += source
          terminated_node_count += 1

        }
        /*
        //terminated_node += source
        //println("Termination node count :"+ terminated_node.length)
        if (terminated_node.length == node_count){
          println("Convergence time = " + (System.currentTimeMillis()-b))
          context.stop(self)
          System.exit(0)
        }
        */
        //terminated_node_count += 1
      }

      case Push_sum_worker_done(sum_estimate:Double) => {
          //context.stop(self)
          //println("Sum estimate is :" +sum_estimate)
            if (sum_estimate - sum_weight_ratio <= 1E-10) {
            println("Convergence time = " + (System.currentTimeMillis() - b))
            context.stop(self)
            System.exit(0)
          }
      }
    }
  }

  class Worker(i:Int,topology:String, algorithm:String, no_nodes_actual:Int, grid_size:Int) extends Actor {

    var neighbor_list = new ListBuffer[String]
    var iterator1 : Int = 0
    val x :Int = 0
    var k :Int = 0
    val grid_size_square = grid_size*grid_size
    var terminate = false
    var num_msg_received = 0
    var rumor:String = null
    var source :String = i.toString
    var sum :Double = i.toDouble
    var weight: Double = 1
    var consecutive_round: Int = 0;
    var gossip_initiated: Boolean = false

    //Ticker which after fixed duration spreads rumor
    override  def preStart() = {
      if (algorithm == "gossip") {
        //Interval set so that worker will start the scheduler
        val interval = (self.path.name.toInt % 10) * 10
        import context.dispatcher
        context.system.scheduler.schedule(0 milliseconds, interval milliseconds, self, Startgossip)
      }
    }

    find_my_neighbors()

    def find_my_neighbors() {
      //println("Entered find neighbors function")
      topology match {
        case "line" => {
          if (i == 0) {
            neighbor_list += (i + 1).toString
          }
          else if (i == no_nodes_actual - 1) {
            neighbor_list += (i - 1).toString
          }
          else {
            neighbor_list += (i - 1).toString
            neighbor_list += (i + 1).toString
          }
        }
        case "full" => {
          for (iterator1 <- 0 to no_nodes_actual - 1) {
            if (iterator1 != i) {
              neighbor_list += iterator1.toString;
              //println("Entered find neighbor full configuration")
            }
          }
        }
        case "3dgrid" => {
          if (i >= 0 && i <= grid_size_square - 1) {
            neighbor_list += (i + grid_size_square).toString
            find_face_neighbor(i, grid_size, 2);
          }
          else if ((i > grid_size_square - 1) && i <= (((grid_size - 1) * grid_size_square) - 1)) {
            neighbor_list += (i - grid_size_square).toString
            neighbor_list += (i + grid_size_square).toString
            if ((i % grid_size_square - 1) != 0) {
              find_face_neighbor(i, grid_size, (i / (grid_size_square - 1)) + 1)
            }
            else {
              find_face_neighbor(i, grid_size, (i / grid_size_square - 1))
            }
          }
          else {
            neighbor_list += (i - grid_size_square).toString
            find_face_neighbor(i, grid_size, 3);
          }
        }
        case "im3dgrid" => {
          if (i >= 0 && i <= grid_size_square - 1) {
            neighbor_list += (i + grid_size_square).toString
            find_face_neighbor(i, grid_size, 1);
          }
          else if ((i >= grid_size_square - 1) && i <= (((grid_size - 1) * grid_size_square) - 1)) {
            neighbor_list += (i - grid_size_square).toString
            neighbor_list += (i - grid_size_square).toString
            //To be modified
            if ((i % grid_size_square) != 0) {
              find_face_neighbor(i, grid_size, (i / (grid_size_square) + 1))
            }
            else {
              find_face_neighbor(i, grid_size, (i / grid_size_square))
            }
          }
          else {
            neighbor_list += (i - grid_size_square).toString
            find_face_neighbor(i, grid_size, grid_size)
          }
          //For random neighbor selection
          neighbor_list += Random.nextInt(no_nodes_actual).toString
        }
      }
    }

    def find_face_neighbor (node_number:Int,grid_size:Int,k:Int) {

      if ((i-grid_size) >0) {
        neighbor_list += (i-grid_size).toString
      }
      if ((i+grid_size) <= (grid_size*grid_size*k - 1)){
        neighbor_list += (i+grid_size).toString
      }

      if ((i%grid_size) == 0 ){
        neighbor_list += (i+1).toString
      }
      else if (i%grid_size == grid_size-1){
        neighbor_list += (i-1).toString
      }
      else{
        neighbor_list += (i+1).toString
        neighbor_list += (i-1).toString
      }
    }

    def get_my_neighbor() :String = {
      val random_neighbor_index:Int = Random.nextInt(neighbor_list.length)
      val random_neighbor:String = neighbor_list(random_neighbor_index)
      return random_neighbor
    }

    def receive = {
      //start_point is gossip sender and the rumor that they send
      case gossip_simulator(start_point, rumor) => {
        //println("Entered gossip simulator")
        num_msg_received += 1
        gossip_initiated = true
        //println("Number of messages received " + num_msg_received)
        this.rumor = rumor
        if (num_msg_received  >= gossip_termination_condition) {
          //println("Did not end yet")
          context.parent ! Gossip_worker_done(source)
          //context.stop(self)
        }
      }

      //Self tick clock for generation of random messages
      case Startgossip => {
        if (gossip_initiated == true) {
          //println("Gossip entered loop")
          val destination: String = get_my_neighbor()
          //println("Destination is" + destination )
          context.actorSelection("../"+ destination) ! gossip_simulator(source, rumor)
        }

      }

      case push_sum_simulator(sum_from_source, weight_from_source) => {
        //num_msg_received += 1

        //println("Entered push sum simulator")
        if (Math.abs((this.sum / this.weight) - ((this.sum + sum_from_source) / (this.weight + weight_from_source))) <= 1E-10)
          consecutive_round += 1
        else
          consecutive_round = 0

        if (consecutive_round == push_sum_termination) {
          context.parent ! Push_sum_worker_done(this.sum / this.weight)
          //context.stop(self)
        }

        this.sum = this.sum + sum_from_source
        this.weight = this.weight + weight_from_source
        this.sum = this.sum / 2
        this.weight = this.weight / 2
        val destination: String = get_my_neighbor()
        context.actorSelection("../" + destination) ! push_sum_simulator(this.sum,this.weight)
      }
    }
  }
}