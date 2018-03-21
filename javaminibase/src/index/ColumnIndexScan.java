package index;

import java.io.IOException;
//import bitmap.BitMapFile;
import btree.*;
import heap.*;
import iterator.*;
import global.*;

public class ColumnIndexScan {

    /**
     * class constructor. set up the index scan.
     * @param index type of the index (B_Index, Hash)
     * @param relName name of the input relation
     * @param indName name of the input index
     * @param types array of types in this relation
     * @param str_sizes array of string sizes (for attributes that are string)
     * @param noInFlds number of fields in input tuple
     * @param noOutFlds number of fields in output tuple
     * @param outFlds fields to project
     * @param selects conditions to apply, first one is primary
     * @param fldNum field number of the indexed field
     * @param indexOnly whether the answer requires only the key or the tuple
     * @exception IndexException error from the lower layer
     * @exception InvalidTypeException tuple type not valid
     * @exception InvalidTupleSizeException tuple size not valid
     * @exception UnknownIndexTypeException index type unknown
     * @exception IOException from the lower layer
     */


    private AttrType       type;
    private IndexType      index;
    private IndexFile      indFile;
    private IndexFileScan  indScan;
    private BTreeFile      btf=null;
    private Scan           bitMapScan=null;
    private AttrType       type1;
    private short          short_size=0;
    private Heapfile       f;
    private Tuple          tuple1;
    private Tuple          Jtuple;
    private boolean        index_only;
    private CondExpr[]     selects;
    private String         relName;

    public String getRelName() {
        return relName;
    }

    public String getIndName() {
        return indName;
    }

    private String indName;
    public CondExpr[] getSelects() {
        return selects;
    }

    public void setSelects(CondExpr[] selects) {
        this.selects = selects;
    }

//	public AttrType getType() {
//		return type;
//	}

    public void setType(AttrType type) {
        this.type = type;
    }

//	public IndexType getIndex() {
//		return index;
//	}

    public void setIndex(IndexType index) {
        this.index = index;
    }


    public ColumnIndexScan(IndexType index,
                           String relName,
                           String indName,
                           AttrType type,
                           short str_sizes,
                           CondExpr[] selects,
                           boolean indexOnly) throws IndexException, UnknownIndexTypeException
    {
        index_only = indexOnly;
        type1 = type;
        short_size= str_sizes;

        try {
            f=new Heapfile(relName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        switch(index.indexType) {

            case IndexType.B_Index:
                try {
                    btf = new BTreeFile(indName);
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, btf);
                }
                catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }
                break;

            case IndexType.None:
                break;

            default:
                throw new UnknownIndexTypeException("Only BTree and Bitmap indexes are supported so far");

        }

        this.setIndex(index);
        this.setSelects(selects);
        this.setType(type);
    }

    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException
    {
        RID rid=null;
        KeyDataEntry nextentry = null;

        if(btf!=null)
        {
            try {
                nextentry = indScan.get_next();
            }
            catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: BTree error");
            }

            if (index_only) {
                // only need to return the key

                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1];

                if (type1.attrType == AttrType.attrInteger) {
                    attrType[0] = new AttrType(AttrType.attrInteger);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setIntFld(1, ((IntegerKey)nextentry.key).getKey().intValue());
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                }
                else if (type1.attrType == AttrType.attrString) {

                    attrType[0] = new AttrType(AttrType.attrString);
                    // calculate string size of _fldNum
                    int count = 0;
                    for (int i=0; i<1; i++) {
                        if (type1.attrType == AttrType.attrString)
                            count ++;
                    }
                    s_sizes[0] = short_size;

                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setStrFld(1, ((StringKey)nextentry.key).getKey());
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                }
                else {
                    // attrReal not supported for now
                    throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                }
                return Jtuple;
            }

            rid = ((LeafData)nextentry.data).getData();
            try {
                tuple1 = f.getRecord(rid);
            }
            catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: getRecord failed");
            }



            return tuple1;

        }
//		    else if(bmf!=null)
//		    {
//		    	try {
//					Tuple t=bitMapScan.getNext(rid);
//					return t;
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//		    }

        return null;
    }

}