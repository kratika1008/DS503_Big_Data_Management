cust = LOAD '/home/arpit/IdeaProjects/DS503/Customers_sample.csv' using PigStorage(',') as (custId:int,Name:chararray,age:int,gender:chararray,countryCode:int,salary:float);
groupedC = GROUP cust by countryCode;
countValC = FOREACH groupedC GENERATE group, COUNT(cust) as c;
filtered = FILTER countValC by c>5000 or c<2000;
store filtered into 'out/Query3' using PigStorage(',');