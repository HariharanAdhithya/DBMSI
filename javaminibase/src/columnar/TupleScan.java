package columnar;

import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class TupleScan {

    public Scan[] scan;
    public Columnarfile cf;

    public TupleScan(Columnarfile columnarfile) {

        int i = 0;
        this.cf = columnarfile;
        this.scan = new Scan[columnarfile.getNumColumns()];
        try {
            for (Heapfile hf: columnarfile.hfColumns) {
                scan[i] = hf.openScan();
                i++;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public void closetuplescan() {

        for(Scan s: scan)
            s.closescan();

        scan = null;

    }

    public Tuple getNext(TID tid) {
        return null;

    }

    public boolean position(TID tid) {
        int i = 0 ;
        try {
            for(Scan s: this.scan) {
                if(!s.position(tid.recordIDs[i]))
                    i++;
                return false;
            }
            return true;
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;

    }
}