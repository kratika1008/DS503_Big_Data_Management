public class Transactions {
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
