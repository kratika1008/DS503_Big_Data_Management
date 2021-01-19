public class Customers {
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
