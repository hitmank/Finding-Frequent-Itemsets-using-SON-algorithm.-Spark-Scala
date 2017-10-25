import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.math.Ordering.Implicits._
import scala.util.Sorting



object karan_son {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("frequent_movies").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val users = sc.textFile(args(2)).map(line => line.split("::"))
    val headerForUsers = Array("UserID", "Gender", "Age", "Occupation", "ZipCode")
    val user_map = users.map(splits => headerForUsers.zip(splits).toMap)

    val text_ratings = sc.textFile(args(1)).map(line => line.split("::"))
    val headerForRatings = Array("UserID", "MovieID", "Rating", "TimeStamp")
    val ratings_map = text_ratings.map(splits => headerForRatings.zip(splits).toMap)

    val supp = args(3).toDouble
    var baskets : RDD[(String,Iterable[String])] = sc.emptyRDD

    if(args(0) == "1") {
      val users_final = user_map.map(each => each.filterKeys(Set("UserID", "Gender")))
      val ratings_final = ratings_map.map(each => each.filterKeys(Set("UserID", "MovieID")))

      //User IDs of all male users
      val male_users = users_final.filter(user => user("Gender") == "M").map(elem => elem("UserID"))
      val male_set = male_users.collect().toSet
      val male_ratings = ratings_final.filter(elem => male_set.contains(elem("UserID")))

      baskets = male_ratings.map(elem => (elem("UserID"), elem("MovieID"))).groupByKey()

    }
    else if(args(0) == "2"){
      val users_final = user_map.map(each => each.filterKeys(Set("UserID", "Gender")))
      val ratings_final = ratings_map.map(each => each.filterKeys(Set("UserID", "MovieID")))

      //User IDs of all male users
      val male_users = users_final.filter(user => user("Gender") == "F").map(elem => elem("UserID"))
      val male_set = male_users.collect().toSet
      val male_ratings = ratings_final.filter(elem => male_set.contains(elem("UserID")))

      baskets = male_ratings.map(elem => (elem("MovieID"), elem("UserID"))).groupByKey()

    }
      // Now start the real work
      var tempOutput = ListBuffer.empty[(List[String])]
      //    performAprioriOnBasket(p,4)
      val numberofbaskets = baskets.count().toDouble

      val output = baskets.mapPartitionsWithIndex((index, x) => {
        var listOfLists = ListBuffer.empty[(List[String])]
        x.toList.map(p => listOfLists += p._2.toList).iterator
        performAprioriOnBasket(listOfLists, index, numberofbaskets, supp).toIterator
      }).reduceByKey((a, b) => 1)
      val v = output.collect()

      val output_counts = baskets.mapPartitionsWithIndex((index, x) => {
        var listOfLists = ListBuffer.empty[(List[String])]
        x.toList.map(p => listOfLists += p._2.toList).iterator
        countRealOccurences(listOfLists, v, index).toIterator
      }).reduceByKey((a, b) => a + b).filter(m => m._2.toDouble >= supp).map(elem => elem._1).sortBy(elem => elem.size)

      output.count()
      output_counts.count()

      var listAns = output_counts.collect()
      listAns = listAns.map(x=>x.sortBy(_.toInt))
      Sorting.quickSort(listAns)(lexicographicOrdering)

      var finalOutput = ""
      var sizeSeen = 1;
      var ts = listAns.map(e => {
        var lala = "(";

        if (e.size > sizeSeen) {
          sizeSeen = e.size
          lala = "\n("
          finalOutput = finalOutput.stripSuffix(", ")
        }
        for ((x, i) <- e.zipWithIndex) {

          lala += x
          if (i != e.size - 1) {
            lala += ", "
          }


        }
        lala += "), "
        finalOutput += lala
        lala
      })
      ts.size
      finalOutput = finalOutput.stripSuffix(", ")
      var outputFile_name = "Karan_Balakrishnan_SON.case"
      if(args(0) == "1"){
        outputFile_name += "1_" + supp.toInt.toString
      }
      else if(args(0) == "2"){
        outputFile_name += "2_" + supp.toInt.toString
      }
      sc.parallelize(List(finalOutput)).map(e => e).coalesce(1, true).saveAsTextFile(outputFile_name)

  }
  object lexicographicOrdering extends Ordering[List[String]] {
    def compare(a:List[String], b:List[String]) = {
      if(a.size == b.size) {
        var ind = 0;
        var found = false
        for ((x, i) <- a.zipWithIndex) {
          if(x != b(i)){
            ind = i;
            found = true
          }
        }
        a(ind).toInt compare b(ind).toInt
      }
      else{
        a.size compare b.size
      }
    }
  }
  def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

  def countRealOccurences(x: ListBuffer[List[String]], cand:Array[(List[String],Int)], part : Int):Array[(List[String],Int)] = {
    var lala = cand.map(y=>{
      var count = 0;

      x.map(g=>{

        if(y._1.forall(g.contains)){

          count += 1
        }

      })


      (y._1,count)

    })
   lala
  }
  def performAprioriOnBasket(x: ListBuffer[List[String]], part: Int, totalBaskets : Double, support : Double):Map[List[String],Int] = {
    val supportForThisPartition = support / (totalBaskets / x.size.toDouble)
    val t = x.flatten.groupBy(identity).map(x => (x._1, x._2.size))
    val candidates = t.filter(x => x._2.toDouble > supportForThisPartition)
    val ret = candidates.map(t => (t._1, 1)) // All size-1 candidates
    val ret2 = candidates.map(t => (List(t._1), 1))
    var setOfCand = ret.map(k => {
      k._1
    }).toList

    var counterw: ListBuffer[(List[String], Int)] = ListBuffer.empty
    ret2.map(k => {
      counterw.append(k)
    })

    var itr = 2;
    var frequentItems = 1;

    do {
      val comb = setOfCand.combinations(itr).toList

      val counter = comb.map(elem => {
        var count = 0;
        x.map(g => {

          if (elem.forall(g.contains)) {
            count += 1;
          }

        })

        (elem.sorted, count)

      })

      val j = counter.filter(x => x._2.toDouble > supportForThisPartition).toMap
      val ha = j.map(h=>h._1)
      setOfCand = ha.flatten.map(j=>j).toList.distinct

      j.map(t => {
        counterw.append(t)
      })

      frequentItems = j.size
      itr += 1

    }while(frequentItems != 0);

    counterw.toMap
  }
}