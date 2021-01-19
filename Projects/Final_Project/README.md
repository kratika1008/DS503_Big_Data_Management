# Final Project: Join Over Skewed Dataset

## Requirements:
* Spark version: 2.4.4
* Scala Version: 2.11.6
* Configured on local system on IntelliJ (for JoinOverSkewedDataset.java)

## Assumptions:
* It is known that one of the dataset is skewed
* There is skewness on a few keys only

## Dataset:
* Custom created Customers and Transaction dataset with few customers having way too many transactions

## Result:
1. Single Repartition Join took: 10.236secs
2. Optimized Join took: 3.792secs
	* Repartition Join of Non-Skewed Customers: 3.77secs
	* Broadcast Join of Skewed Customers: 0.0195secs

* Optimization saved approximately 60% of the time for computation, eliminating the imbalance from Reducers	