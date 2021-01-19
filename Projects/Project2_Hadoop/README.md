# Project 2: Hadoop

## Requirements:
* Hadoop Version: Hadoop 2.7.3

1. Spatial Join:
	* CreatePointsDataset.java (used to generate Points.txt file)
	* CreateRectangleDataset.java (used to generate Rectangle.txt file)
	* GenerateGrids.java (used to generate grids of given size, given an entire space)
	* Problem1.java (mapreduce job for Spatial join)
	-----------------------------------------------------------------------------------------------
	-> Argument 0: path of Point.txt file <br/>
	-> Argument 1: path for Rectangle.txt file <br/>
	-> Argument 2: path of the output folder <br/>
	-> Argument 3: String format window parameter with value as “x1,y1,x2,y2”, where x1,y1 determine the bottom-left coordinate of the window and x2,y2 define the top-right coordinate of the window. <br/>
2. K-means Clustering:
	* KmeansPoints.java (used to generate KmeansPoints.file)
	* PointsDistance.java (used to calculate the distance between two points in 2D plane)
	* Problem2.java (mapreduce job for K-means clustering)
	-----------------------------------------------------------------------------------------------
	-> Argument 0: path of K centroid file i.e. KmeansPoints.txt <br/>
	-> Argument 1: path of Point.txt file <br/>
	-> Argument 2: path of the output folder <br/>
3. Custom Input Format:
	* JSONInputFormat.java (used to read JSON Input file to Text Input File format)
	* Problem3.java (mapreduce job for analysing Custom Input Format)
	-----------------------------------------------------------------------------------------------
	-> Argument 0: path of airfield.json file <br/>
	-> Argument 1: path of the output of converted text file folder <br/>
	-> Argument 2: path of the final output folder with Maximum & Minimum Elevation grouped by Flags <br/>
4. Distance-Based Outlier Detection Clustering:
	* CreatePointsDataset.java (used to generate Points.txt file)
	* PointsDistance.java (used to calculate the distance between two points in 2D plane)
	* GenerateGrids.java (used to generate grids of given size, given an entire space)
	* Problem4.java (mapreduce for Distance based Outlier detection)
	------------------------------------------------------------------------------------------------
	-> Argument 0: path of Point.txt file <br/>
	-> Argument 1: path of the output folder <br/>
	-> Argument 2: threshold(k) String value (converts it later to an integer value) <br/>
	-> Argument 3: threshold(r) String value (converts it later to a float value) <br/>
	
	