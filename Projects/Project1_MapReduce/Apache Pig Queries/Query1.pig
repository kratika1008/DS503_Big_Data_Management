txn = LOAD '/home/arpit/IdeaProjects/DS503/Transactions_sample.csv' using PigStorage(',') as (txnId:int,custId:int,transTotal:float,transNumItem:int);
cust = LOAD '/home/arpit/IdeaProjects/DS503/Customers_sample.csv' using PigStorage(',') as (custId:int,Name:chararray,age:int,gender:chararray,countryCode:int,salary:float);
groupedT = GROUP txn by custId;
T1= FOREACH groupedT GENERATE group,COUNT(txn.txnId) AS no_of_txn;
T2 = JOIN cust by custId, T1 by group;
Result1 = FOREACH T2 GENERATE Name, no_of_txn;
Result2 = ORDER Result1 by no_of_txn;
STORE Result2 into 'out/query1' using PigStorage(',');