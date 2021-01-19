package DS503_finalProject;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.Writer;
import java.io.FileWriter;

public class CreateDataset {

    private static final String[] genders={"male","female"};
    private static final String Cust_FileHeader = "ID,Name,Age,Gender,CountryCode,Salary";
    private static final String Trans_FileHeader = "TransID,CustID,TransTotal,TransNumItems,TransDesc";
    public static void main(String [] args) throws Exception
    {
        Random rand = new Random();
        List<Customers> lstCustomers = new ArrayList<Customers>();
        int totalCust = 100000;
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
        int totalTrans = 13000000;
        for(int i=1;i<=totalTrans;i++){
            int CustID = 0;
            //In order to generate skewness in Transaction data over customerId=7426,5,98775,384,67452,2486
            if((i%2 != 0) && i<3000000)
                CustID = 7426;
            else if(i<=2000000)
                CustID = 5;
            else if((i>6000000 && i<7500000) && (i%2 ==0))
                CustID = 98775;
            else if(i>9000000 && i<10000000)
                CustID = 384;
            else if(i>10000000 && i<10900000)
                CustID = 67452;
            else if((i%2 != 0) && (i>11000000 && i<12806792))
                CustID = 2486;
            else
                CustID = lstCustomers.get(rand.nextInt(lstCustomers.size())).getID();
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
            csvWriter.append(Cust_FileHeader);
            csvWriter.append("\n");

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
            csvWriter.append(Trans_FileHeader);
            csvWriter.append("\n");

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

class Customers {
   private int ID;
   private String Name;
   private int Age;
   private String Gender;
   private int CountryCode;
   private Float Salary;

   public Customers(int ID, String Name, int Age, String Gender, int CountryCode, Float Salary){
       this.ID = ID;
       this.Name = Name;
       this.Age = Age;
       this.Gender = Gender;
       this.CountryCode = CountryCode;
       this.Salary = Salary;
   }

   public int getID() {
       return ID;
   }
   public String getName() {
       return Name;
   }
   public int getAge() {
       return Age;
   }
   public String getGender() {
       return Gender;
   }
   public int getCountryCode() {
       return CountryCode;
   }
   public Float getSalary() {
       return Salary;
   }
}

class GenerateRandom {
    public static String generateRandomString(Random rand,int minLen,int maxLen){
        int l_Limit = 97; //char a
        int r_Limit = 122; //char z
        int len = rand.nextInt(maxLen - minLen +1) + minLen;
        StringBuilder buffer = new StringBuilder(len);
        for (int j = 0; j < len; j++) {
            int randomLimitedInt = l_Limit + (int)
                    (rand.nextFloat() * (r_Limit - l_Limit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String stringValue = buffer.toString();
        return stringValue;
    }
    public static int getRandomNumber(Random rand,int minVal,int maxVal){
        int value = rand.nextInt(maxVal - minVal +1) + minVal;
        return value;
    }
}


 class Transactions {
    private int TransID;
    private int CustID;
    private float TransTotal;
    private int TransNumItems;
    private String TransDesc;

    public Transactions(int TransID, int CustID, float TransTotal, int TransNumItems, String TransDesc){
        this.TransID = TransID;
        this.CustID = CustID;
        this.TransTotal = TransTotal;
        this.TransNumItems = TransNumItems;
        this.TransDesc = TransDesc;
    }

    public int getTransID() {
        return TransID;
    }
    public int getCustID() {
        return CustID;
    }
    public Float getTransTotal() {
        return TransTotal;
    }
    public int getTransNumItems() {
        return TransNumItems;
    }
    public String getTransDesc() {
        return TransDesc;
    }
}