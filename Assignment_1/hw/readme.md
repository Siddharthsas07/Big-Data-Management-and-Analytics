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
-> Here, I am assuming to be in /sas151830 directory and my hw1 folder is under /sas151830/BigData/hw1 folder

Q1 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/UniqueBuzCategory.jar hw1/Q1UniqueBuzCategory /sas151830/input/business.csv /sas151830/output1

input files	: /hw1/input
output file	: /hw1/output1
jar file	: UniqueBuzCategory
class		: hw1/Q1UniqueBuzCategory
====================================================

Q2 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/BestRatedBuz10.jar hw1/Q2BestRatedBuz10 /sas151830/input/review.csv /sas151830/output2

input files	: /hw1/input
output file	: /hw1/output2
jar file	: BestRatedBuz10
class		: hw1/Q2BestRatedBuz10
====================================================

Q3 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/TopTenBuzDetails.jar hw1/Q3TopTenBuzDetails /sas151830/input/review.csv /sas151830/input/business.csv /sas151830/intermidiate /sas151830/output3

input files	: /hw1/input
output file	: /hw1/output3
jar file	: TopTenBuzDetails
class		: hw1/Q3TopTenBuzDetails
====================================================

Q4 : Command line
---------------------------------------------------
hadoop jar BigData/hw1/UserRatings.jar hw1/Q4UserRatings /sas151830/input/review.csv /sas151830/input/business.csv /sas151830/output4

input files	: /hw1/input
output file	: /hw1/output4
jar file	: UserRatings
class		: hw1/Q4UserRatings
====================================================