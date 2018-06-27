package comp9313.ass4
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

object SetSimJoin {
  def main(args: Array[String]){
    // get input file, output folder, and threshold
    val inputFile = args(0)
    val outputFolder = args(1)
    val threshold = args(2).toDouble
    
    // initialize Spark Configuration
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    
    // get input and split
    val input = sc.textFile(inputFile)
    val line_data = input.map(line => line.split(" "))

    ///// STAGE 1 /////
    // order token based on frequency in the whole datasets
    // logic:
    // 1. mapReduce (token, count) <- basic Token ordering
    // 2. swap ordered (count, token) and then group by count <- name it Global Token Ordering
    // 3. now go through the whole recordID and then map it in ascending order based on the Global Token Ordering List
    
    // basic token ordering
    val bto = line_data.flatMap{ x =>
      for (i <- 1 until x.length) yield{
        (x(i).toInt, 1)
      }
    }.reduceByKey(_ + _)
    .map{case(key, count) => (count, key)}.sortBy(x => x._2)
    .groupByKey()
    .sortByKey()
    
    // global token ordering
    val gto = bto.collect.toList.map(_._2)
    
    // update line_data to new data with ordered token
    val ordered_data = line_data.map{ x =>
      var rec = x(0)
      val list = new ListBuffer[Int]
      for (i <- 1 until x.length){
        list += x(i).toInt
      }
      val ordered = new ListBuffer[Int]
      for (i <- gto){
        for (j <- i){
          for (k <- list){
            if (j == k){
              ordered += k
            }
          }
        }
      }
      
      // ordered list is now having token in sorted order
      for (i <- ordered){
        rec += " " + i.toString()
      }
      (rec)
    }.map(line => line.split(" "))
    // end of Stage 1, now go to stage 2
    
    ///// STAGE 2 /////
    // get the Record ID Pair generation
    // apply prefix filter initially
    // group together based on the prefix filtered result
    // apply length filtering, and then positional filtering
    // emit the pairs that survives the filter
    
    // map line data into Key Value pairs : (prefix, recordIDList)
    // recordIDList = nodeID|<elementID>
    val data = ordered_data.flatMap{ x =>
      var recIDList = x(0).toString() + "|"
      for (i <- 1 until x.length){
        var temp = x(i).toString() + ","
        recIDList += temp
      }
      
      recIDList = recIDList.dropRight(1)
      
      // get prefix length
      // |set| - (|set| * t) + 1
      val prefixLength = (x.length - 1) - math.ceil((x.length - 1) * threshold.toDouble).toInt + 1
      
      for ( i <- 1 to prefixLength) yield{
        (x(i), recIDList)
      }
    }
    
    // group together based on prefix filtered result
    val grouped = data.map{ case (a, b) => (a, ListBuffer[String](b))}
    .reduceByKey(_++=_)
    
    // based on the grouped result, generate Record ID Pairs
    // apply filter first to reduce the pairs generated
    val reduced = grouped.flatMap( {case(key, value) =>
     value.toList.combinations(2).map({ k => 
       val first = (k(0).split('|')(0), k(0).split('|')(1))
       val second = (k(1).split('|')(0), k(1).split('|')(1))
       
       val first_idList = first._2.split(',')
       val second_idList = second._2.split(',')
       
       // apply length filter
       // calculation:
       // min(|x|) <= |y| <= max(|y|)
       val minLength = threshold * first_idList.length
       val maxLength = first_idList.length / threshold
       // length filter
       // if passed go through the next filter
       // if failed, we prune it
       if (minLength <= second_idList.length && second_idList.length <= maxLength){
         // apply positional filter
         // calculation:
         // get alpha value: ceiling(t / 1+t) * (|x| + |y|)
         // get upperbound : overlap + min(|x| - positionX, |y| - positionY)
         val a = math.ceil((threshold/(1 + threshold)) * (first_idList.length + second_idList.length))
         var intersect = 0.0
         var overlap = 0
         var x_pos = 0
         var y_pos = 0
         
         for (i <- 0 until first_idList.length){
           if (first_idList(i) == key){
             x_pos = i
           }
         }
         
         for (i <- 0 until second_idList.length){
           if (second_idList(i) == key){
             y_pos = i
           }
         }
         
         // get current overlap & intersect         
         for (i <- 0 until first_idList.length; j <- 0 until second_idList.length){
           if (first_idList(i) == second_idList(j)){
             if (i <= x_pos && j <= y_pos){
               overlap += 1
             }
             intersect += 1.0
           }

         }
         
         val ubound = overlap + math.min(first_idList.length - (x_pos + 1), second_idList.length - (y_pos + 1))
         // positional filter
         // if passed get the jaccardSimilarity and apply final filter
         // if fail we prune it
         if (a <= ubound){
           // calculate jaccard similarity
           // calculation:
           // intersect(x, y) / union(x, y)
           val union = first_idList.length + second_idList.length - intersect
           val jaccardSim = intersect / union
           // if jaccardSim >= threshold emit result
           if (jaccardSim >= threshold){
             ((first._1.toInt, second._1.toInt), jaccardSim)
           } else{
             // prune
             ((-1, -1), 0.0)
           }
         } else{
           // prune
           ((-1, -1), 0.0)
         }
       } else {
         // prune
         ((-1, -1), 0.0)
       }
     })
    })
    
    ///// STAGE 3 /////
    // remove all pruned result
    // get the distinct key
    val processed = reduced.filter(_ != ((-1, -1), 0.0)).reduceByKey((a, b) => a)
    
    // sort based on key
    // and map to output folder
    // format:
    // key \t jaccardSim
    val simiJoin = processed.sortBy(x => (x._1._1, x._1._2))
    .map( x => x._1 + "\t" + x._2)
    
    simiJoin.saveAsTextFile(outputFolder)
  }
}