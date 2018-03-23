package BitMap;


import java.io.*;
import btree.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import columnar.*;
import value.*;
import heap.*;

public class BitMapFile implements GlobalConst {
	private final static int MAGIC0=1989;

	private final static String lineSep=System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	public static void traceFilename(String filename) 
			throws  IOException
	{

		fos=new FileOutputStream(filename);
		trace=new DataOutputStream(fos);
	}
	public static void destroyTrace() 
			throws  IOException
	{
		if( trace != null) trace.close();
		if( fos != null ) fos.close();
		fos=null;
		trace=null;
	}

	private BMHeaderPage headerPage;
	private  PageId  headerPageId;
	private String  dbname;  

	public BMHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename)         
			throws GetFileEntryException
	{
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e,"");
		}
	}
	private void add_file_entry(String fileName, ColumnarFile columnfile, PageId pageno) 
			throws AddFileEntryException
	{
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, columnfile, pageno)//filename and its PGID is added to DB.
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e,"");
		}      
	}


	private Page pinPage(PageId pageno) 
			throws PinPageException
	{
		try {
			Page page=new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			return page;// return the pg if not pinned.
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e,"");
		}
	}

	private void unpinPage(PageId pageno) 
			throws UnpinPageException
	{ 
		try{
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		} 
	}

	private void freePage(PageId pageno) 
			throws FreePageException
	{
		try{
			SystemDefs.JavabaseBM.freePage(pageno);    
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e,"");
		} 

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException
	{
		try {	
			SystemDefs.JavabaseDB.delete_file_entry( filename );
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e,"");
		} 
	}

	private void unpinPage(PageId pageno, boolean dirty) 
			throws UnpinPageException
	{
		try{
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);  
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		}  
	}
	//Constructor when file already exist;
	public BitMapFile (String filename)
			throws GetFileEntryException,
			PinPageException,
			ConstructPageException
	{
		headerPageId=get_file_entry(filename);

		headerPage=new BMHeaderPage(headerPageId);
		dbname = new String(filename);

	}

	//Constructor to create a new file, if it doesnt exist; 
	public BitMapFile( String filename, ColumnarFile  columnfile, int columno, ValueClass value)
			throws GetFileEntryException, 
			ConstructPageException,
			IOException, 
			AddFileEntryException {

		headerPageId=get_file_entry(filename);

		if( headerPageId==null) //file not exist
		{
			headerPage= new  BMHeaderPage(); 
			headerPageId= headerPage.getPageId();
			add_file_entry(filename,columnfile, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_ColNo(columno);
			headerPage.setValue(value);
			headerPage.setType(NodeType.BTHEAD);
		}
		else {
			headerPage = new BMHeaderPage(headerPageId);  
		}

		dbname=new String(filename);
		
		if(value instanceof IntegerValue)
		{ int intkeyval = ((IntegerValue)value).getValue();
		accessInt(columnfile,columno,intkeyval);
		}
		else if(value instanceof StringValue) {
			String strkeyval = ((StringValue)value).getValue();
			accessStr(columnfile,columno,strkeyval);}



	}

	public void accessStr(ColumnarFile columnfile, int columno, String Value) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {

		Tuple t = new Tuple();
		int position = 0;

		RID rid = new RID();
		Scan columnScan = columnfile.openColumnScan(columno);


		while ((t = columnScan.getNext(rid)) != null) {
			String colVal = t.getStrFld(1);
			if(colVal.equals(Value)) {
				insert(position);
			} else {
				delete(position);
			}
			position++;
		}

	}
	
	public void accessInt(ColumnarFile columnfile, int columno, int Value) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {

		Tuple t = new Tuple();
		int position = 0;

		RID rid = new RID();
		Scan columnScan = columnfile.openColumnScan(columno);


		while ((t = columnScan.getNext(rid)) != null) {
			int colVal = t.getIntFld(1);
			if(colVal == Value) {
				insert(position);
			} else {
				delete(position);
			}
			position++;
		}

	}


	public void close()
			throws PageUnpinnedException, 
			InvalidFrameNumberException, 
			HashEntryNotFoundException,
			ReplacerException
	{
		if ( headerPage!=null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);//wtf??
			headerPage=null;
		}  
	}

	public boolean insert (int position)
			throws IOException {
		//BMPage page = new BMPage();
		PageId apage = new PageId();

		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			BMPage page = new BMPage();
			page.setABit(position,1);
			return true;

		}

		if(headerPage.get_rootId().pid != INVALID_PAGE) {
			BMPage page = new BMPage();
			apage = page.getCurPage();
			page.openBMpage(page);
			byte [] data;
			if(page.getavailable_space() != 0) {
				data = page.getBMpageArray();
				int count = page.getCount();
				if(position > count+1)
					return false;
				else {
					page.setABit(position, 1);
					return true;
				}
			}
			else {
				BMPage page1= new BMPage();
				PageId  apage1 = new PageId();
				page.setNextPage(apage1);
				page1.setCurPage(apage1);
				page1.setPrevPage(apage);
				page1.openBMpage(page1);
				byte [] data2;
				if(page1.getavailable_space() != 0) {
					data2 = page1.getBMpageArray();
					int count2 = page1.getCount();
					if(position > count2+1)
						return false;
					else {
						page.setABit(position, 1);
						return true;
					}

				}

			}
		}		 

	}

	public boolean delete (int position)
			throws IOException {

		PageId apage = new PageId();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			BMPage page = new BMPage();
			if(page.getavailable_space() == NULL) 
			{
				page.setBit(position,0);
				return true;
			}
		}
		else {

			BMPage page = new BMPage();
			apage = page.getCurPage();
			page.openBMpage(page);
			byte [] data;
			data = page.getBMpageArray();
			int count = page.getCount();
			if(position > count+1)
				return false;
			else
				page.setBit(position, 0);
			return true;
		}


	}

}