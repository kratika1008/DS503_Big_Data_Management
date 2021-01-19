import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.Writer;
import java.io.FileWriter;

public class CreateDatasets {

  public static final String[] genders={"male","female"};
  public static void main(String [] args) throws Exception
  {
    Random rand = new Random();
    List<Customers> lstCustomers = new ArrayList<Customers>();
    int totalCust = 50000;
    for(int i=1;i<=totalCust;i++){
      String custName = GenerateRandom.generateRandomString(rand,10,20);
      int custAge = GenerateRandom.getRandomNumber(rand,10,70);
      String custGender = genders[rand.nextInt(genders.length)];
      int custCountryCode = GenerateRandom.getRandomNumber(rand,1,10);
      Float custSalary = GenerateRandom.getRandomNumber(rand,100,10000)*rand.nextFloat();
      lstCustomers.add(new Customers(i,custName,custAge,custGender,custCountryCode,custSalary));
    }
    createCustCSV(lstCustomers);
    List<Transactions> lstTransactions = new ArrayList<Transactions>();
    int totalTrans = 5000000;
    for(int i=1;i<=totalTrans;i++){
      int CustID = lstCustomers.get(rand.nextInt(lstCustomers.size())).getID();
      float TransTotal = GenerateRandom.getRandomNumber(rand,10,1000)*rand.nextFloat();
      int TransNumItems = GenerateRandom.getRandomNumber(rand,1,10);
      String TransDesc = GenerateRandom.generateRandomString(rand,20,50);
      lstTransactions.add(new Transactions(i,CustID,TransTotal,TransNumItems,TransDesc));
    }
    createTransCSV(lstTransactions);
  }
  public static void createCustCSV(List<Customers> custData){
    try
    {
      FileWriter csvWriter = new FileWriter("Customers.csv");
      for (Customers row: custData) {
        csvWriter.append(String.valueOf(row.getID()));
        csvWriter.append(",");
        csvWriter.append(row.getName());
        csvWriter.append(",");
        csvWriter.append(String.valueOf(row.getAge()));
        csvWriter.append(",");
        csvWriter.append(row.getGender());
        csvWriter.append(",");
        csvWriter.append(String.valueOf(row.getCountryCode()));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(row.getSalary()));
        csvWriter.append("\n");
      }
      csvWriter.flush();
      csvWriter.close();
    }
    catch (Exception e) {
      System.out.println("Error in Customer CsvFileWriter !!!");
      e.printStackTrace();
    }
  }
  public static void createTransCSV(List<Transactions> transData){
    try
    {
      FileWriter csvWriter = new FileWriter("Transactions.csv");
      for (Transactions row: transData) {
        csvWriter.append(String.valueOf(row.getTransID()));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(row.getCustID()));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(row.getTransTotal()));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(row.getTransNumItems()));
        csvWriter.append(",");
        csvWriter.append(row.getTransDesc());
        csvWriter.append("\n");
      }
      csvWriter.flush();
      csvWriter.close();
    }
    catch (Exception e) {
      System.out.println("Error in Transaction CsvFileWriter !!!");
      e.printStackTrace();
    }
  }


}
