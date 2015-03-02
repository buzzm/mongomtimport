
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import com.mongodb.WriteConcern;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Calendar;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileReader;
import java.io.StringReader;
import java.io.BufferedReader;
import java.io.RandomAccessFile;
import java.io.IOException;


/**
 *  Given a JSON import file e.g.
 *
 *  $ ls -l /tmp/z
 *  -rw-r--r--  1 buzz  wheel  103888890 Feb 26 12:50 /tmp/z
 *  $ head -1 /tmp/z
 *  {"systemID": "sysA", "ents": {"description": "D", "productType": "A"}, "userID": 0, "batch": "B1"}
 *  $ wc -l /tmp/z
 *  1000000 /tmp/z
 *
 *  mongomtimport will create n threads and assign each thread a region of
 *  the file to import.
 *  Strategy:
 *  1. Divide total bytes by n threads, e.g. 1973 / 4 = 493.25
 *  2. Start at position 493 and walk backward until CR or 0 is found; this
 *     is pos x1
 *  3. If x1 is 0, this means 493 was still in the middle of an entry and
 *     n is too small; set n = n - 1 and goto #1
 *  4. Move to position 493 *2 and walk backward until CR or x1 is found; this
 *     is pos x2
 *  
 *
 *
 *
 */

public class mongomtimport {

    private Map params = new HashMap();

    private DBCollection coll;
    private int bulksize;
    private int fileType;
    private List<FieldType> ftypes;
    private List<FieldColumn> fcols;
    private DB db;
    private boolean trim = false;
    private MongoImportHandler handler = null;
    private MongoImportParser parser = null;

    private char separator = ',';  // not final because could change...

    private WriteConcern wc = null;
    private boolean fetchInsertCount = true;

    private final static int JSON_TYPE = 0;
    private final static int DELIM_TYPE = 1;
    private final static int FIXED_TYPE = 2;
    private final static int CUSTOM_TYPE = 3;

    private final static int STRING_TYPE = 1;
    private final static int LONG_TYPE   = 2;
    private final static int INT_TYPE    = 3;
    private final static int DATE_TYPE   = 4;
    private final static int OID_TYPE    = 5;
    private final static int BIN_TYPE    = 6;
    private final static int DBL_TYPE    = 7;

    private final static int STRING_L_TYPE = -1;
    private final static int LONG_L_TYPE   = -2;
    private final static int INT_L_TYPE    = -3;
    private final static int DATE_L_TYPE   = -4;
    private final static int OID_L_TYPE    = -5;
    private final static int BIN_L_TYPE    = -6;
    private final static int DBL_L_TYPE    = -7;





    // # of DBObjects to put in bulkops buffer before calling execute()
    private final static int DEFAULT_BULKSIZE = 16;

    private void reportError(String s, boolean forceExit) {
	reportError(s);
	System.exit(1);
    }

    private void reportError(String s) {
	p("error: " + s);
	if((boolean)params.get("stopOnError")) {
	    // Likely a more graceful way than this but...
	    System.exit(1); 
	}
    }

    private void pp(String s) {
	if((boolean) params.get("verbose")) {
	    System.out.println(s);
	}
    }

    private void p(String s) { System.out.println(s); }

    private void usage() {
	p("usage: mongomtimport -d db -c collection [ options ] importFile [ importfile ... ]");
	p("importFile is a CR-delimited JSON, delimited, or fixed-width file.");
	p("File will be divided down by n threads and a starting region assigned to each thread.");
	p("Each region will be processed by a thread, parsing the CR-delimited JSON and adding");
	p("the DBObject to a bulk operations buffer.  When the bulksize is reached, the bulk insert is executed.");
	p("Smallish document sizes (~1K bytes) will benefit from larger bulksize");
	p("Default threads is 1 and default bulksize is " + DEFAULT_BULKSIZE);
	p("");
	p("If only one importFile is specified and it names a directory, then ALL files in");
	p("that directory will be processed.  All options will apply to all the files");
	p("processed in that directory");
	p("Empty lines (CR only) are permitted but will be ignored");

	p("");
	p("options:");
	p("--host | -h     name of host e.g. machine.firm.com (default localhost");
	p("--port          port number (default 27017)");
	p("--username | -u    user name (if auth required for DB)");
	p("--password | -p    password (if auth required for DB)");

	p("");
	p("-d dbname       name of DB to load (default test)");
	p("-c collname     name of collection in DB to load (default test)");

	p("");
	p("--parseOnly       perform all parsing actions but do NOT effect the target DB (no connect, no drop, no insert)");
	p("--drop            drop collection before inserting");
	p("--type json|delim|fixed   identify type of file (json is default)");
	p("--separator c     char to use as field and embedded list (see *List types) (default is comma)");
	p("--trim            (fixed and delim only) post-parse, strip leading and trailing whitespace from items.");
	p("                  If the field is trimmed to size 0, it will not be added.");
	p("                  Is a noop if a custom parser is in use.");
	p("                  Highly recommended for fixed length imports.");
	p("--threads n       number of threads amongst which to divide the read/parse/insert logic");
	p("--bulksize n      size of bulkOperations buffer for each thread");
	p("--stopOnError     do not try to continue if parsing/insert error occurs");
	p("--writeConcern    JSON doc of write concern options e.g. {\"w\": 0, \"wtimeout\": 0, \"fsync\": false, \"j\": false}");


	// FIELDS
	p("");
	p("--fieldPrefix str    (noop for JSON) If --fields not present OR number of items on current line > spec in");
	p("                     --fields, then name the field str%d where %d is the zero-based index of the item");

	p("");
	p("--fields spec    (noop for JSON) spec is fldName[:fldType[:fldFmt]][, ...]");
	p("                 fldType is optional and is string by default");
	p("                 fldType one of string, int, long, double, date, binary00");
	p("                 OR above with List appended e.g. stringList");
	p("                 e.g. --fields 'name,age:int,bday:date:YYYYMMDD'");
	p("                 Each item on line will be named fldName and the string value converted to the");
	p("                 specified fldType.  *List types are special; items in the line must be quoted");
	p("                 delimited and will be further split and the result assigned to an array.  So given:");
	p("                        steve,\"dog,cat\"  ");
	p("                   --fields \"name,pets\"  will produce { name: steve, pets: \"dog,cat\"} ");
	p("                 but");
	p("                   --fields \"name,pets:stringList\" will produce { name: steve, pets: [ \"dog\", \"cat\"] }");
	p("                 fldType date formats: YYYY-MM-DD (default), YYYYMMDD, YYMMDD (<70 add 2000 else add 1900), YYYYMM (assume day 1), ISO8601");
	p("                 ISO8601 format accepts milliseconds.  If timezone is not explicitly declared");
	p("                 using [+-]HHMM or Z (Z means +0000) then the timezone of the running process will be");
	p("                 taken into account");

	p("");
	p("--fieldColumns   (fixed type only and required) A comma separated list of start and end position pairs where 1 (not 0) is the first column.");
	p("                 Dash (e.g. 3-6) defines start and end position, inclusive");
	p("                 Plus (e.g. 3+4) defines start position and length");
	p("                 Single columns may be represented without the dash or plus.");
	p("                     --fieldColumns 3-5,6-9,24,30-45");
	p("                 is equivalent to");
	p("                     --fieldColumns 3+3,6+4,24,30+16");

	p("                 Use --fields to name and type extracted columns otherwise default behavior is same as delimited file.");
	p("                 *List fields will use the separator character to further break up extracted columns");




	//  HANDLER!
	p("");
	p("--handler classname      load a class using standard classloader semantics that implements MongoImportHandler:");
	p("                        public interface MongoImportHandler {");
	p("                          void init(String fileName);");
	p("                          boolean process(java.util.Map);");
	p("                          void done(String fileName);");   
	p("                        }");
	p("                        init() is called once at the start of each file to be processed.  ");
	p("                        process() is called with the parsed Map (post-mongoDB JSON type convention conversion)");
	p("                        and any operation can be performed on the map.  If process() returns false the map ");
	p("                        will not be inserted.");
	p("                        done() is called once following processing of all items.");
	p("                         handler is called by all threads so take care to add synchronization where necessary.");



	//  PARSER!
	p("");
	p("--parser classname      load a class using standard classloader semantics that implements MongoImportParser:");
	p("                        public interface MongoImportParser {");
	p("                          void init(String fileName);");
	p("                          boolean process(String, java.util.Map);");
	p("                          void done(String fileName);");   
	p("                        }");
	p("                        init() is called once at the start of each file to be processed.  ");
	p("                        process() is called with the UNparsed line of data read from the file and ");
	p("                        an empty Map.  Any logic can be performed to load name-value content into the map.");
	p("                        If process() returns false this signals the parser failed to process the line of data.");
	p("                        done() is called once following processing of all items.");
	p("                        parser is called by all threads so take care to add synchronization where necessary.");
	p("                        parser overrides the --type option.");



	p("--verbose        chatty output");
    }


    private static class FieldColumn {
	private int startIdx; // zero based internally...
	private int len; 

	public FieldColumn(int startIdx, int len) {
	    this.startIdx = startIdx;
	    this.len = len;
	}
    }

    private static class FieldType {
	private String name;
	private int type;
	private String fmt;

	public FieldType(String name, int type) {
	    this.name = name;
	    this.type = type;
	    this.fmt = null;
	}
	public FieldType(String name, int type, String fmt) {
	    this.name = name;
	    this.type = type;
	    this.fmt = fmt;
	}
    }



    private class Q implements Callable<Integer> {
	private long startPos;
	private long endPos;
	private String fname;
	private int tnum;

	/**
	 *  Open and read file from startPos to endPos.
	 *  If endPos = -1, read until EOF 
	 *  Read a line of JSON, convert to a Map, and insert.
	 */
	public Q(int tnum, String fname, long startPos, long endPos) {
	    this.tnum = tnum;
	    this.fname = fname;
	    this.startPos = startPos;
	    this.endPos = endPos;
	}

	public Integer call() {
	    pp("tnum " + this.tnum + "; startpos " + this.startPos + "; endpos " + this.endPos);

	    String s = null;
	    int numRows = 0;

	    try {
		long qpos = this.startPos;

		java.util.Date start = new java.util.Date();
		
		FileReader fr = new FileReader(this.fname);
		BufferedReader buffrd = new BufferedReader(fr);
		
		BulkWriteOperation bo = null;
		//List<DBObject> inslist = new ArrayList<DBObject>(bulksize);

		s = null;

		int rr = 0;

		buffrd.skip(this.startPos);

		int maxFtypes = 0;
		if(ftypes != null) {
		    maxFtypes = ftypes.size();
		}

		String fldpfx = (String) params.get("fieldPrefix");

		boolean parseOnly = (boolean) params.get("parseOnly");


		while((s = buffrd.readLine()) != null) {
		    Map<String,Object> mm = null;

		    if(s.equals("")) {
			continue;
		    }

		    if(fileType == DELIM_TYPE) {
			CSVReader csvr = null;

			csvr = new CSVReader(new StringReader(s), separator);

			String[] toks = csvr.readNext();
			if(toks != null) {
			    mm = new HashMap(); 
			    for(int jj = 0; jj < toks.length; jj++) {
				String tok = toks[jj];
				
				if(trim) {
				    tok = tok.trim();
				}

				if(tok.length() == 0) {
				    continue;
				}
				if(ftypes != null && jj < maxFtypes) {
				    FieldType ft = ftypes.get(jj);
				    Object o = processItem(ft, tok);
				    if(o != null) {
					mm.put(ft.name, o);
				    }
				} else {
				    mm.put(fldpfx + jj, tok);
				}
			    }
			}

		    } else if(fileType == FIXED_TYPE) {
			mm = new HashMap(); 
			int nfcol = fcols.size();
			int maxs = s.length();

			for(int jj = 0; jj < nfcol; jj++) {
			    int eidx = (fcols.get(jj).startIdx + fcols.get(jj).len);
			    if(eidx > maxs) {
				eidx = maxs;
			    }

			    //System.out.println(jj + ": " + fcols.get(jj).startIdx + "," + eidx);
			    String tok = s.substring(fcols.get(jj).startIdx, eidx);
			    
			    if(trim) {
				tok = tok.trim();
			    }

			    if(tok.length() == 0) {
				continue;
			    }

			    if(ftypes != null && jj < maxFtypes) {
				FieldType ft = ftypes.get(jj);
				Object o = processItem(ft, tok);
				if(o != null) {
				    mm.put(ft.name, o);
				}
			    } else {
				mm.put(fldpfx + jj, tok);
			    }
			}
			
		    } else if(fileType == JSON_TYPE) {
			try {
			    JsonParser2 p = new JsonParser2(s);
			    Object value = p.parse(true);
			    if(value == null) {
				reportError("content " + s + " parses to null");
			    }
			    mm = (Map<String,Object>) value;

			} catch(Exception e) {
			    reportError("content " + s + ": " + e);
			}

		    } else if(fileType == CUSTOM_TYPE) {
			mm = new HashMap(); 
			boolean ok = parser.process(s, mm);
			if(!ok) {
			    mm = null; // signal to logic below...
			}
		    }

		    /*
		     *  By this point, if a map has not been created, then 
		     *  something is bad and we should move on to the next
		     *  line...
		     */
		    if(mm == null) {
			continue;
		    }

		    numRows++;


		    /**
		     *  DATABASE WRITE SECTION
		     */
		    if(!parseOnly) {
			if(bo == null) {
			    bo = coll.initializeUnorderedBulkOperation();
			}
			{
			    boolean doit = true;
			    
			    if(handler != null) {
				doit = handler.process(mm);
			    }
			    
			    if(doit == true) {
				BasicDBObject bdo = new BasicDBObject(mm); // AUUGH!
				
				bo.insert(bdo);
				if(0 == numRows % bulksize) {
				    BulkWriteResult rc = bo.execute(wc);
				    if(fetchInsertCount) {
					rr += rc.getInsertedCount();
				    }
				    bo = null;
				}
			    }
			}
		    }


		    // Only need to stop if we are NOT the last
		    // chunk going thru...
		    if(this.endPos != -1) {
			//long qpos = raf.getFilePointer();
			qpos += (s.length() + 1);

			if(qpos == this.endPos) {
			    //System.out.println("  reached " + qpos);
			    break; // reached the next marker
			}
		    }
		} // end of while(read input) loop


		if(bo != null) {
		    BulkWriteResult rc = bo.execute(wc);
		    if(fetchInsertCount) {
			rr += rc.getInsertedCount();
		    }
		    bo = null;
		}

		buffrd.close();

		java.util.Date end = new java.util.Date();
		long diff = end.getTime() - start.getTime();
		double psec = ((double)numRows / ((double)diff / 1000));

		{
		    String s4 = " wrote ";
		    if(!fetchInsertCount) {
			s4 = " probably wrote ";
			rr = numRows;
		    }
		    pp(this.tnum + s4 + rr + " of " + numRows + " in " + diff + " ms; " + (int)psec + " ins/sec");
		}

	    } catch(Exception e) {
		System.out.println("!!!: some sort of fail in thread: " + e + "; last doc read: " + s);
		e.printStackTrace();
	    }

	    return new Integer(numRows);
	}

    }


    public static void main(String[] args) {
	mongomtimport z = new mongomtimport();
	z.go(args);
    }


    public void go(String[] args) {

	try {
	    int nthreads = 1; // default is 1 thread

	    bulksize = DEFAULT_BULKSIZE;

	    List<String> files = new ArrayList<String>();

	    params.put("parseOnly", false);

	    params.put("host", "localhost");
	    params.put("port", 27017);

	    params.put("verbose", false);
	    params.put("stopOnError", false);
	    params.put("coll", "test");
	    params.put("db", "test");
	    params.put("drop", false);
	    params.put("type", JSON_TYPE);
	    params.put("fieldPrefix", "field");


	    for(int j = 0; j < args.length; j++) {
		String a = args[j];
		if(a.equals("-d")) {
		    j++;
		    params.put("db", args[j]);

		} else if(a.equals("-c")) {
		    j++;
		    params.put("coll", args[j]);

		} else if(a.equals("--threads")) {
		    j++;
		    nthreads = Integer.parseInt(args[j]);

		} else if(a.equals("-h") || a.equals("--host")) {
		    j++;
		    params.put("host", args[j]);

		} else if(a.equals("--port")) {
		    j++;
		    params.put("port", Integer.parseInt(args[j]));

		} else if(a.equals("-u") || a.equals("--username")) {
		    j++;
		    params.put("uname", args[j]);

		} else if(a.equals("-p") || a.equals("--password")) {
		    j++;
		    params.put("pw", args[j]);

		} else if(a.equals("--bulksize")) {
		    j++;
		    bulksize = Integer.parseInt(args[j]);

		} else if(a.equals("--drop")) {
		    params.put("drop", true);

		} else if(a.equals("--verbose")) {
		    params.put("verbose", true);

		} else if(a.equals("--parseOnly")) {
		    params.put("parseOnly", true);

		} else if(a.equals("--fields")) {
		    j++;
		    params.put("fields", args[j]);

		} else if(a.equals("--fieldColumns")) {
		    j++;
		    params.put("fieldColumns", args[j]);

		} else if(a.equals("--handler")) {
		    j++;
		    params.put("handler", args[j]);

		} else if(a.equals("--parser")) {
		    j++;
		    params.put("parser", args[j]);

		} else if(a.equals("--fieldPrefix")) {
		    j++;
		    params.put("fieldPrefix", args[j]);

		} else if(a.equals("--stopOnError")) {
		    params.put("stopOnError", true);


		} else if(a.equals("--writeConcern")) {
		    j++;
		    JsonParser2 p = new JsonParser2(args[j]);
		    Object value = p.parse(true);
		    if(value == null) {
			reportError("--writeConcern " + args[j] + " parses to null", true);
		    }
		    params.put("writeConcern", (Map)value);
		    
		} else if(a.equals("--trim")) {
		    trim = true;

		} else if(a.equals("--type")) {
		    j++;
		    String s = args[j].toLowerCase();
		    if(s.equals("json")) {
			params.put("type", JSON_TYPE);
		    } else if(s.equals("delim")) {
			params.put("type", DELIM_TYPE);
		    } else if(s.equals("fixed")) {
			params.put("type", FIXED_TYPE);
		    } else {
			reportError("unknown --type option [" + a + "]", true);
		    }

		} else if(a.equals("--separator")) {
		    j++;
		    String s = args[j].toLowerCase();
		    separator = s.charAt(0); // grab a single char!

		} else if(a.equals("--help") || a.equals("-help")) {
		    usage();
		    System.exit(0);

		} else if(a.startsWith("--")) {
		    reportError("unknown option [" + a + "]", true);

		} else {
		    // Not a -- arg; must be an importfile
		    files.add(a);
		}
	    }


	    if(! (boolean) params.get("parseOnly")) {
		MongoClient mongoClient = null;

		ServerAddress sa = new ServerAddress((String)params.get("host"),(Integer)params.get("port"));

		String dbname = (String)params.get("db");

		/**
		   Unclear if we need to go to this level yet.  More than
		   100 threads on one import seems...heavy.

		    MongoClientOptions options = MongoClientOptions.builder()
			.connectionsPerHost(100)
			.build();
		**/
		
		{
		    String uname = (String)params.get("uname");
		    String pw = (String)params.get("pw");
		    if(uname != null && pw != null) {
			char[] pwch = pw.toCharArray();
			MongoCredential mc = MongoCredential.createPlainCredential(uname, dbname, pwch);
			List<MongoCredential> mcs = new ArrayList<MongoCredential>();
			mcs.add(mc);
			
			mongoClient = new MongoClient(sa, mcs);
			
			if(mongoClient == null) {
			    reportError("name+password supplied not valid for database " + dbname, true);
			}
		    } else {
			mongoClient = new MongoClient(sa);
		    }
		}

		DB db = mongoClient.getDB(dbname);

		coll = db.getCollection( (String)params.get("coll") );

	    } else {
		pp("parseOnly mode; no database actions will be taken");
	    }


	    { // WriteConcern treatment
		// Stolen from --verbose output on new go-based mongoimport
		int wc_w         = 1;
		int wc_wtimeout  = 0;
		boolean wc_fsync = false;
		boolean wc_j     = false;

		Map pwc = (Map) params.get("writeConcern");
		if(pwc != null) {
		    // {w: 3, wtimeout: 500, fsync: true, j: 1}
		    if(pwc.containsKey("w")) {
			wc_w = (Integer) pwc.get("w");
			if(wc_w == 0) {
			    fetchInsertCount = false;
			}
		    }
		    if(pwc.containsKey("wtimeout")) {
			wc_wtimeout = (Integer) pwc.get("wtimeout");
		    }
		    if(pwc.containsKey("fsync")) {
			wc_fsync = (Boolean) pwc.get("fsync");
		    }
		    if(pwc.containsKey("j")) {
			wc_j = (Boolean) pwc.get("j");
		    }
		}

		wc = new WriteConcern(wc_w, wc_wtimeout, wc_fsync, wc_j);

		pp("using writeConcern w:" + wc_w 
		   + ",wtimeout:" + wc_wtimeout
		   + ",fsync:" + wc_fsync
		   + ",j:" + wc_j
		   );
	    }



	    long totProc = 0;

	    int n = nthreads;  

	    java.util.Date start = new java.util.Date();

	    /***
		For now, a simple simplification.  If one file given,
		sniff to see if it is a dir.   If so, extract all the
		files out of it and prep...
	    ***/
	    if(files.size() == 1) {
		String dirname = files.get(0);
		File probe = new File(dirname);
		if (probe.isDirectory()) {
		    File[] listOfFiles = probe.listFiles();
		    if(listOfFiles!=null) {
			files.clear();
			for (int i = 0; i < listOfFiles.length; i++) {
			    files.add(dirname + "/" + listOfFiles[i].getName());
			}
			pp("processing " + files.size() + " files from directory " + dirname);
		    }
		}
	    }

	    fileType = (int) params.get("type");

	    if(fileType == FIXED_TYPE) {
		String ff = (String) params.get("fieldColumns");
		if(ff != null) {
		    fcols = processColumns(ff);
		} else {
		    reportError("--type fixed requires --fieldColumns parameter", true);
		}	
	    }


	    ftypes = null;
	    if(fileType == DELIM_TYPE || fileType == FIXED_TYPE) {
		String ff = (String) params.get("fields");
		if(ff != null) {
		    ftypes = processFields(ff);
		}
	    }



	    {
		String handlerClassName = (String) params.get("handler");
		if(handlerClassName != null) {
		    Object o = Class.forName(handlerClassName).newInstance();
		    if(o != null) {
			handler = (MongoImportHandler)o;
			pp("handler " + handlerClassName + " set up");
		    } else {
			pp("handler " + handlerClassName + " FAILED to load");
		    }
		}
	    }
	    {
		String parserClassName = (String) params.get("parser");
		if(parserClassName != null) {
		    Object o = Class.forName(parserClassName).newInstance();
		    if(o != null) {
			parser = (MongoImportParser)o;
			pp("parser " + parserClassName + " set up");
			fileType = CUSTOM_TYPE;
		    } else {
			pp("parser " + parserClassName + " FAILED to load");
		    }
		}
	    }



	    /***
	     *   OK.  Pretty much everything that could go wrong upon setup is
	     *   behind us.
	     *   So now it is OK to drop the collection...
	     */
	    if(! (boolean) params.get("parseOnly")) {
		if((boolean)params.get("drop")) {
		    pp("dropping " + (String)params.get("coll"));
		    coll.drop();
		}
	    }

	    for(int kk = 0; kk < files.size(); kk++) {
		String fname = files.get(kk);
		pp("working on " + fname);
		if(parser != null) { parser.init(fname); }
		if(handler != null) { handler.init(fname); }
		totProc += processFile(fname, nthreads);
		if(handler != null) { handler.done(fname); }
		if(parser != null) { parser.done(fname); }
	    }

	    java.util.Date end = new java.util.Date();
	    long diff = end.getTime() - start.getTime();

	    pp(totProc + " items; millis: " + diff);
	    double psec = ((double)totProc / ((double)diff / 1000));
	    pp("performance " + (int)psec + " ins/sec");

	} catch (Exception e) {
	    System.out.println("go() epic fail: " + e);
	    e.printStackTrace();
	}
    }


    private Object processItem(FieldType ft, String input) 
	throws IOException
    {
	Object ox = null;

	switch(ft.type) {
	case STRING_TYPE:
	    ox = input;
	    break;

	case DATE_TYPE:
	    ox = cvtDate(input, ft.fmt);
	    break;

	case INT_TYPE:
	    try {
		ox = Integer.parseInt(input);
	    } catch(NumberFormatException ne) {
		// Double.valueOf(input) will decode scientific notation....
		ox = Integer.valueOf(Double.valueOf(input).intValue());
	    }

	    break;

	case LONG_TYPE:
	    try {
		ox = Long.parseLong(input);
	    } catch(NumberFormatException ne) {
		// Double.valueOf(input) will decode scientific notation....
		ox = Long.valueOf(Double.valueOf(input).longValue());
	    }
	    break;

	case DBL_TYPE:
	    ox = Double.valueOf(input);
	    break;

	case OID_TYPE:
	    break;

	case BIN_TYPE: {
	    byte[] vv = javax.xml.bind.DatatypeConverter.parseBase64Binary(input); 	    // YOW!
	    ox = vv;
	    break;
	}

	case STRING_L_TYPE: { // optimized performance here...
	    CSVReader xx = new CSVReader(new StringReader(input), separator);
	    String[] vv = xx.readNext();
	    ox = vv;
	    break;
	}

	case INT_L_TYPE: 
	case LONG_L_TYPE: 
	case DBL_L_TYPE: 
	case DATE_L_TYPE:
	case BIN_L_TYPE:
	{
	    CSVReader xx = new CSVReader(new StringReader(input), separator);
	    String[] vv = xx.readNext();
	    List ix = new ArrayList(vv.length);

	    /***
	    System.out.print("input: ["  + input + "]");
	    for(String bb : vv) {
		System.out.print(" ["  + bb + "]");
	    }
	    System.out.println("");
	    ***/

	    // "Convert" arrayType to scalar by * -1!
	    FieldType sft = new FieldType("_", (ft.type * -1), ft.fmt);

	    for(int kk = 0; kk < vv.length; kk++) {
		ix.add(processItem(sft, vv[kk]));
	    }
	    ox = ix;
	    break;
	}
	}

	return ox;
    }



    private List<FieldType> processFields(String ff) {
	List<FieldType> fl = null;

	try {
	    CSVReader csvr = new CSVReader(new StringReader(ff));
	    String[] flds = csvr.readNext();
	    fl = new ArrayList<FieldType>();
	    for(int kk = 0; kk < flds.length; kk++) {
		String fn = flds[kk];
		String tn = "string";
		String fm = null; // no format hint

		int tt = STRING_TYPE;

		if(fn.contains(":")) {
		    CSVReader csvr2 = new CSVReader(new StringReader(fn), ':');
		    String[] ftt = csvr2.readNext();
		    fn = ftt[0];
		    tn = ftt[1];
		    if(ftt.length == 3) {
			fm = ftt[2];
		    }
		}
		if(tn.equals("string")) {
		    tt = STRING_TYPE;
		} else if(tn.equals("date")) {
		    tt = DATE_TYPE;
		} else if(tn.equals("int")) {
		    tt = INT_TYPE;
		} else if(tn.equals("long")) {
		    tt = LONG_TYPE;
		} else if(tn.equals("double")) {
		    tt = DBL_TYPE;
		} else if(tn.equals("oid")) {
		    tt = OID_TYPE;
		} else if(tn.equals("binary00")) {
		    tt = BIN_TYPE;
		} else if(tn.equals("stringList")) {
		    tt = STRING_L_TYPE;
		} else if(tn.equals("dateList")) {
		    tt = DATE_L_TYPE;
		} else if(tn.equals("intList")) {
		    tt = INT_L_TYPE;
		} else if(tn.equals("longList")) {
		    tt = LONG_L_TYPE;
		} else if(tn.equals("oidList")) {
		    tt = OID_L_TYPE;
		} else if(tn.equals("binary00List")) {
		    tt = BIN_L_TYPE;
		} else {
		    reportError("unknown field type [" + tn + "]", true);
		}

		fl.add(new FieldType(fn, tt, fm));
	    }
	} catch(Exception e) {
	    System.out.println("processField() epic fail: " + e);
	}

	return fl;
    }


    /**
     *   1-20,35-40,42,45-
     */
    private List<FieldColumn> processColumns(String ff) {
	List<FieldColumn> fl = null;

	try {
	    CSVReader csvr = new CSVReader(new StringReader(ff));
	    String[] flds = csvr.readNext();

	    fl = new ArrayList<FieldColumn>();
	    for(int kk = 0; kk < flds.length; kk++) {
		String fn = flds[kk];

		int startIdx = -1;
		int len = -1;
		char sep = 0;

		if(fn.contains("-")) {
		    sep = '-';
		} else if(fn.contains("+")) {
		    sep = '+';
		}

		if(sep != 0) {
		    CSVReader csvr2 = new CSVReader(new StringReader(fn), sep);
		    String[] ftt = csvr2.readNext();
		
		    startIdx = Integer.parseInt(ftt[0]);
		    len = Integer.parseInt(ftt[1]);
		} else { // is a single!
		    startIdx = Integer.parseInt(fn);
		    len = 1;
		}

		if(sep == '-') {
		    // 3-6  means 4 total, so (len-startIdx)+1
		    len = (len - startIdx) + 1;
		} 
		// noop for sep = '-'

		startIdx--;  // back up to 0-based idx instead of 1-based pos

		fl.add(new FieldColumn(startIdx, len));
	    }
	} catch(Exception e) {
	    reportError("processField() fail: " + e, true);
	}

	return fl;
    }

    private long processFile(String fname, int nthreads) {

	long startPos[] = new long[nthreads];
	long tot = 0; // total docs processed

	try {
	    // RandomAccessFile out of the JDK has a HIDEOUS implementation
	    // but fortunately we're not really using it heavily -- only
	    // to search for a few CR.
	    RandomAccessFile raf = new RandomAccessFile(fname, "r");

	    long totb = raf.length();

	    if(totb == 0) {
		System.out.println("error: [" + fname + "] is zero length");
		System.exit(1);
	    }

	    long pos = raf.getFilePointer();
	    long chunk = totb / nthreads;

	    pp(fname + " is " + totb);
	    startPos[0] = 0;
		
	    for(int j = 1; j < nthreads; j++) {
		long npos = chunk * j;
		raf.seek(npos);

		pp("chunk start at " + npos);

		byte cc = 0;
		long loops = 0;

		/**
		 *  This algo is rather inefficient, especially for larger
		 *  documents where you may have to back up several thousand
		 *  bytes.  But the cost of doing this is still much much
		 *  less than actually parsing the JSON and calling insert
		 *  for what is likely to be a great deal of docs.
		 */
		while(true) {
		    cc = raf.readByte();
		    if(cc == '\n') {
			break;
		    }
		    npos -= 1;
		    if(npos == 0) {
			break;
		    }
		    raf.seek(npos);
		    loops++;
		}

		pp(loops + " loops to find CR");
		if(cc == '\n') {
		    pp("CR found at " + npos);
		    startPos[j] = npos + 1; // look beyond the CR!
		}
	    }

	    raf.close();


	    /**
	     *  At this point, startPos[] is loaded with the offsets.
	     *  Spawn multiple threads and go!
	     */
	    pp("running " + nthreads + " threads, bulksize " + bulksize);
	    
	    ExecutorService es = Executors.newCachedThreadPool();	     
	    Set<Future<Integer>> set = new HashSet<Future<Integer>>();
	    
	    for(int k = 0; k < nthreads; k++) {
		long endPos = -1;
		if(k != nthreads-1) {
		    endPos = startPos[k+1];
		}
		if(endPos == startPos[k]) {
		    pp("bypass");
		    continue;
		}
		
		Callable<Integer> callable = new Q(k, fname, startPos[k], endPos);
		Future<Integer> future = es.submit(callable);
		set.add(future);
		
		// Add a small bit of wait time to not overload the 
		// mongoDB client with "instant threads"
		try{
		    Thread.currentThread().sleep(50); // twentieth of a second
		}
		catch(InterruptedException ie){
		    // ... keep going
		}
	    }
	    
	    for(Future<Integer> future : set) {
		tot += future.get();
	    }
	    
	    
	    es.shutdown();
	    boolean finshed = es.awaitTermination(1, TimeUnit.MINUTES);
	    
	} catch (Exception x) {
	    System.out.println("proc file exception: " + x);
	}
	
	return tot;
    }




    private static Date cvtDate(String s, String fmt) {

	Date d = null;
	Calendar cal = null;
	int year  = 0;
	int month = 0;
	int day   = 0;

	// Organize logic for faster decisions, so most common
	// things up front...

	if(fmt == null || fmt.equals("YYYY-MM-DD")) {
	    year  = Utils.buildNumber(s, 0, 4);
	    month = Utils.buildNumber(s, 5, 2);
	    day   = Utils.buildNumber(s, 8, 2);

	} else if(fmt.equals("YYYYMMDD")) {
	    year  = Utils.buildNumber(s, 0, 4);
	    month = Utils.buildNumber(s, 4, 2);
	    day   = Utils.buildNumber(s, 6, 2);

	} else if(fmt.equals("YYMMDD")) {
	    year  = Utils.buildNumber(s, 0, 2);
	    if(year < 70) { 
		year += 2000; // 140203, 650203 -> 3-Feb-2014, 3-Feb-2065
	    } else { 
		year += 1900; // 700203, 960203 -> 3-Feb-1970, 3-Feb-1996
	    }
	    month = Utils.buildNumber(s, 2, 2);
	    day   = Utils.buildNumber(s, 4, 2);

	} else if(fmt.equals("YYYYMM")) { // 201405 becomes 1-May-2014
	    year  = Utils.buildNumber(s, 0, 4);
	    month = Utils.buildNumber(s, 4, 2);
	    day   = 1; // YOW!
	}

	if(year != 0) {
	    cal = Calendar.getInstance();
	    cal.clear();
	    cal.set( Calendar.YEAR,  year );
	    cal.set( Calendar.MONTH, month - 1);
	    cal.set( Calendar.DATE,  day ); 
	    d = cal.getTime();					
	    
	} else if(fmt != null && fmt.equals("ISO8601")) {
	    d = Utils.cvtISODateToDate(s);
	}

	return d;
    }
}
