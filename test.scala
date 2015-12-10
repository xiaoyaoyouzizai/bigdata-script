val file = sc.textFile("hdfs://192.168.1.140/data/9hong/orders.csv")
// val fltr = file.filter(_.length > 0)
// file.toArray.foreach(println)
// val keys = fltr.map(_.split(",")).map(a => a(5))
// keys.collect().foreach(println)
// myLines_filtered.count()
val stateSum = file.map(_.split(",")).map(key => (key(23).substring(1,14)+key(1)+key(11), (key(13).substring(1,key(13).length-1).toFloat*10000).toInt))

// stateSum.toArray.foreach(println)
// stateSum.collect().foreach(println)
// val lastMap = stateSum.countByKey
// val lastMap = stateSum.foldLeft(0)(_+_._2)

val lastMap = stateSum.groupBy(_._1).mapValues(_.map(_._2).sum)
lastMap.saveAsTextFile("result2")