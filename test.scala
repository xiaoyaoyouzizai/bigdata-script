val ordersRDD = sc.textFile("hdfs://192.168.1.140/data/9hong/orders.csv")
val testUsersRDD = sc.textFile("hdfs://192.168.1.140/data/9hong/test_users.csv").flatMap(_.split(",")).map(k=>(k,null))

val userOrdersRDD = ordersRDD.map(_.split(",")).map(key => (key(6).substring(1,key(6).length-1), (key(23).substring(1,14) + "," + key(1).substring(1,key(1).length-1) + "," + key(11).substring(1,key(11).length-1), key(13).substring(1,key(13).length-1))))
val sumRDD = userOrdersRDD.subtractByKey(testUsersRDD).map(x=>(x._2._1,x._2._2.toFloat)).reduceByKey((x,y)=>x+y)


sumRDD.saveAsTextFile("sum.csv")
