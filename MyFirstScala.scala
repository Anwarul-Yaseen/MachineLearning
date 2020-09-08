val document = sc.textFile("wordsearch.txt")
val words = document.filter(line => line.contains("CHAPTER"))
words.count()
//(or)
var query = "Hadoop"
document.filter(line => line.contains(query)).count()
