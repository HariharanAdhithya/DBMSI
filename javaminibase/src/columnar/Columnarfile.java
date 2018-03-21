package columnar;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.RID;
import global.TID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

public class Columnarfile {

    private int numColumns;
    private AttrType[] type;

    public int tupleLength;
    public int delCount;
    public String [] hfNames;
    public String [] colNames;
    public Heapfile[] hfColumns;

    public Heapfile deletedTupleList;

    public int getNumColumns() {
        return numColumns;
    }

    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }

    public AttrType[] getType() {
        return type;
    }

    public void setType(AttrType[] type) {
        this.type = type;
    }

    public Columnarfile() {
        // TODO Auto-generated constructor stub
    }

    public Columnarfile(String name, int numColumns, AttrType[] type) {

        this.setType(type);
        this.setNumColumns(numColumns);
        this.setType(type);
        this.tupleLength = 0;
        this.delCount = 0;

        hfNames = new String [numColumns];
        hfColumns = new Heapfile[numColumns];
        colNames = new String [numColumns];

        int i = 0;
        try {
            for (AttrType t: type) {
                hfNames[i] = name.concat(Integer.toString(i));
                hfColumns[i] = new Heapfile(hfNames[i]);

                if(t.attrType == AttrType.attrInteger) {
                    tupleLength = tupleLength + 4;
                }
                else if (t.attrType == AttrType.attrString) {
                    tupleLength = tupleLength + 32;
                }
                i++;
            }
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void deleteColumnarFile() {
        try {
            for (Heapfile h: this.hfColumns) {
                h.deleteFile();
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public TID insertTuple(byte[] tuplePtr) {

        int curPos = 0;
        TID tid = new TID(numColumns);

        int i = 0;
        try {
            for (AttrType t: type) {
                if (t.attrType == AttrType.attrInteger)	{

                    int intAttr = Convert.getIntValue(curPos,tuplePtr);
                    curPos = curPos + 4;

                    byte[] value = new byte[4];
                    Convert.setIntValue(intAttr, 0, value);

                    tid.recordIDs[i] = new RID();
                    tid.recordIDs[i] = hfColumns[i].insertRecord(value);
                }
                if (t.attrType == AttrType.attrString)	{

                    String strAttr = Convert.getStrValue(curPos,tuplePtr,32);
                    curPos = curPos + 32;

                    byte[] value = new byte[32];
                    Convert.setStrValue(strAttr, 0, value);

                    tid.recordIDs[i] = new RID();
                    tid.recordIDs[i] = hfColumns[i].insertRecord(value);
                }

                i++;
            }
            tid.numRIDs = i;
            //tid.position =

        }
        catch (Exception e)	{
            e.printStackTrace();
        }
        return tid;

    }

    public Tuple getTuple(TID tid) {

        byte[] tuple = new byte[tupleLength];
        int offset = 0;

        Tuple t = new Tuple();

        try {
            for (int i= 0; i < numColumns ; i++) {

                t = hfColumns[i].getRecord(tid.recordIDs[i]);

                if (type[i].attrType == AttrType.attrInteger) {
                    int value = Convert.getIntValue(offset,t.returnTupleByteArray());
                    Convert.setIntValue(value,offset,tuple);
                    offset = offset + 4;
                }

                if (type[i].attrType == AttrType.attrString) {
                    String value = Convert.getStrValue(offset,t.returnTupleByteArray(),32);
                    Convert.setStrValue(value,offset,tuple);
                    offset = offset + 32;
                }
            }

            t.tupleSet(tuple,0,tuple.length);

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return t;

    }

    public ValueClass getValue(TID tid, int column) {

        ValueClass value = null;

        IntegerValue i= new IntegerValue();
        StringValue str= new StringValue();

        try{
            byte[] colValue = hfColumns[column].getRecord(tid.recordIDs[column]).returnTupleByteArray();

            if (type[column].attrType == AttrType.attrInteger)	{

                i.setValue(Convert.getIntValue(0,colValue));
                value = i;
            }
            else if (type[column].attrType == AttrType.attrString)	{

                str.setValue(Convert.getStrValue(0,colValue,32));
                value = str;
            }
        }catch(Exception e)
        {
            e.printStackTrace();
        }
        return value;

    }

    public int getTupleCnt() {

        int count = 0;
        try {
            count = hfColumns[0].getRecCnt();
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
                | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return count;
    }

    public TupleScan openTupleScan() {

        TupleScan scan = new TupleScan(this);
        return scan;

    }

    public Scan openColumnScan(int columnNo) {

        Scan scan = null;
        try {
            scan = new Scan(hfColumns[columnNo]);
        } catch (InvalidTupleSizeException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return scan;

    }

    public boolean updateTuple(TID tid, Tuple newtuple) {

        int i = 0;

        for (;i<numColumns;i++) {
            if(!updateColumnofTuple(tid,newtuple,i+1))
                return false;
        }
        return true;

    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) {

        int intValue;
        String strValue;
        Tuple tuple = null;
        try {
            if (type[column-1].attrType == AttrType.attrInteger)	{
                intValue = newtuple.getIntFld(column);
                tuple = new Tuple(4);
                tuple.setIntFld(1, intValue);
            }
            else if (type[column-1].attrType == AttrType.attrString)	{
                strValue = newtuple.getStrFld(column);
                tuple = new Tuple(32);
                tuple.setStrFld(1, strValue);
            }

            return hfColumns[column-1].updateRecord(tid.recordIDs[column-1], tuple);

        }catch (Exception e)	{
            e.printStackTrace();
        }
        return false;

    }

    public boolean createBTreeIndex(int column) {
        return true;

    }

    public boolean createBitMapIndex(int columnNo, ValueClass value) {
        return true;

    }

    public boolean markTupleDeleted(TID tid) {

        byte[] deletedTids = new byte[numColumns*4*2];

        int i = 0;
        int offset = 0;
        int tidOffset = 0;


        try{
            for (AttrType attr: type) {
                if(attr.attrType == AttrType.attrInteger)
                {
                    Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidOffset, deletedTids);
                    Convert.setIntValue(tid.recordIDs[i].slotNo, tidOffset + 4, deletedTids);

                    offset = offset + 4;
                    tidOffset = tidOffset + 8;
                    if(!hfColumns[i].deleteRecord(tid.recordIDs[i]))
                        return false;
                    i++;
                }
                else if(attr.attrType == AttrType.attrString)
                {
                    Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidOffset, deletedTids);
                    Convert.setIntValue(tid.recordIDs[i].slotNo, tidOffset + 4, deletedTids);

                    offset = offset + 32;
                    tidOffset = tidOffset + 8;
                    if(!hfColumns[i].deleteRecord(tid.recordIDs[i]))
                        return false;
                    i++;
                }
            }
            deletedTupleList.insertRecord(deletedTids);
            this.delCount++;
            return true;
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;

    }

    public boolean purgeAllDeletedTuples() {
        return false;


    }

}