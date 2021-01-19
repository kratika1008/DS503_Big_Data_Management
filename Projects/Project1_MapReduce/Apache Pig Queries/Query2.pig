txn = LOAD '/home/arpit/IdeaProjects/DS503/Transactions_sample.csv' using PigStorage(',') as (txnId:int,custId:int,transTotal:float,transNumItem:int);
cust = LOAD '/home/arpit/IdeaProjects/DS503/Customers_sample.csv' using PigStorage(',') as (custId:int,Name:chararray,age:int,gender:chararray,countryCode:int,salary:float);
joinC = JOIN cust  BY custId, txn BY custId using 'replicated';
groupedC = GROUP joinC by (cust::custId,cust::Name,cust::salary);
countValC = FOREACH groupedC GENERATE group, COUNT(joinC),SUM(joinC.transTotal),MIN(joinC.transNumItem);
store countValC into 'out/Query2' using PigStorage(',');



