# Finding-frequent-itemsets-using-SON-algorithm.Built-on-SPARK-with-Scala
Implementation of SON algorithm on Spark with Scala

# Versions used : Spark 1.6.1
# Scala version 2.10 sbt version 0.13.6.
# Class Name: karan_son

# Command to run:
./bin/spark-submit --class karan_son –master local[*] <path_to_jar> <case_number> <path_to_ratings.dat> <path_to_users.dat> <support>
# Example:
./bin/spark-submit --class karan_son –master local[*] /users/karan/artifacts/Karan_Balakrishnan_SON.js 1 /users/karan/ratings.dat /users/karan/users.dat 1200
# How did I implement this:
1. For both the cases, my first step was to read the input files (users and ratings) and to create the baskets.
2. Thus, for case 1, my baskets were created as a “Map [Male Users -> Movies]” and for case 2, my baskets were created as a “Map [Movies -> Female Users]”
3. Step 1 of the SON algorithm involves dividing the baskets into chunks and processing parallelly. I did the partitioning with “mapPartitions”.
4. For each partition, I sent the chunk of baskets to a method called “performAprioriOnBasket” which first counted the frequent singles, then pairs, then triples, etc. till no frequent items are found.
5. The support was different for each partition. It was calculated with the formula: Total Support / size of partition.
6. After this first map phase, each partition returned a List of Candidate items that it thought to be frequent. The reduce phase would just collect these items and pass them on.
7. The second map partition phase took all the candidate items given by the previous reduce and called a method called: “countRealOccurences”, which counted how many times each of the candidates appeared in the partition.
8. This counting was done by checking if the candidate item list is a sub list of the basket.
9. The result of each map partition is then passed to the reducer, which initiall will sum all
the counts for each item from all the maps, and then filter those that are less than the real support value.
