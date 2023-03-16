
import java.io.IOException;
import java.util.*;

public class Stream {
    private final String rowFilter;
    private final String columnFilter;
    private final String valueFilter;
    private bigT bigtable;
    private int type, orderType;
    public Heapfile tempHeapFile;
    private String tempHeapFileName = null;


    public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter) {
    
        this.bigtable = bigTable;
        this.orderType = orderType;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.tempHeapFileName = this.bigtable.name + "tempSort4" + random.nextInt(1000);
        this.tempHeapFile = new Heapfile(tempHeapFileName);
    }
  
    public void closeStream(){
    
        if (this.sortObj != null) {
            this.sortObj.close();
        }
        if (mapScan != null) {
            mapScan.closescan();
        }
        if (btreeScanner != null) {
            btreeScanner.DestroyBTreeFileScan();
        }
    }

    public Map getNext(){
        MiniTable.orderType = orderType;
        if (this.sortObj == null) {
            System.out.println("sort object is not initialised");
            return null;
        }

        Map m = null;

        try {
            m = this.sortObj.get_next();
    
        } catch (OutOfSpaceException e) {
            System.out.println("outofspace");
            e.printStackTrace();
            closeStream();
        }
        if (m == null) {
            tempHeapFile.deleteFile();
            closeStream();
            return null;
        }
        return m;
    }
    
    public String getBigTName() {
        return this.bigtable.name;
    }
}
