Assignment - 1
--------------------------------------------------

All files are placed inside hw1 directory.
Files are placed as below:

-> 4 jar files 	: /hw1/
-> input files 	: /hw1/input/
-> output files	: /hw1/output<number>/
-> java files	: /hw1/package/
---------------------------------------------------

HOW TO RUN EACH QUESTION:

-> First place all package to your current local directory.
-> Here, I am assuming you are familiar with the path in hdfs directories. All hdfs path are specified as <path>. Other path are local to the linux or any other system you are using.

Q1 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/UniqueBuzCategory.jar hw1/Q1UniqueBuzCategory <business.csv> <output1>

input files	: /hw1/input
output file	: /hw1/output1
jar file	: UniqueBuzCategory
class		: hw1/Q1UniqueBuzCategory
====================================================

Q2 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/BestRatedBuz10.jar hw1/Q2BestRatedBuz10 <review.csv> <output2>

input files	: /hw1/input
output file	: /hw1/output2
jar file	: BestRatedBuz10
class		: hw1/Q2BestRatedBuz10
====================================================

Q3 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/TopTenBuzDetails.jar hw1/Q3TopTenBuzDetails <review.csv> <business.csv> <intermidiatePath> <output3>

input files	: /hw1/input
output file	: /hw1/output3
jar file	: TopTenBuzDetails
class		: hw1/Q3TopTenBuzDetails
====================================================

Q4 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/UserRatings.jar hw1/Q4UserRatings <review.csv> <business.csv> <output4>

input files	: /hw1/input
output file	: /hw1/output4
jar file	: UserRatings
class		: hw1/Q4UserRatings
====================================================