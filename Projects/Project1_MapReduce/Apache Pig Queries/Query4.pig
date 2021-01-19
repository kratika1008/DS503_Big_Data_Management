txn = LOAD '/home/arpit/IdeaProjects/DS503/Transactions_sample.csv' using PigStorage(',') as (txnId:int,custId:int,transTotal:float,transNumItem:int);
cust = LOAD '/home/arpit/IdeaProjects/DS503/Customers_sample.csv' using PigStorage(',') as (custId:int,Name:chararray,age:int,gender:chararray,countryCode:int,salary:float);

y = foreach cust generate
    (age>=10 and age<20?'10_20':(age>=20 and age<30?'20_30':(age>=30 and age<40?'30_40':(age>=40 and age<50?'40_50':(age>=50 and age<60?'50_60':(age>=60 and age<=70?'60_70':'')))))) as agerange,
    age, gender, custId;

joined = join y by custId, txn by custId;

grp1= group joined by (agerange,gender);

grp2 = foreach grp1 generate group, SUM(joined.transTotal) as S, COUNT(joined.txnId) as N, MIN(joined.transTotal) as min_t,  MAX(joined.transTotal) as max_t;
grp3 = foreach grp2 generate group, S/N, min_t,  max_t;

store grp3 into 'out/Query4' using PigStorage(',') ;
