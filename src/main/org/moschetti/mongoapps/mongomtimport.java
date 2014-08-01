
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

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
    private int listsize;
    private int fileType;
    private List<FieldType> ftypes;
    private DB db;
    private MongoImportHandler handler = null;
    private MongoImportParser parser = null;


    private final static int JSON_TYPE = 0;
    private final static int CSV_TYPE = 1;
    private final static int CUSTOM_TYPE = 2;

    private final static int STRING_TYPE = 1;
    private final static int LONG_TYPE   = 2;
    private final static int INT_TYPE    = 3;
    private final static int DATE_TYPE   = 4;
    private final static int OID_TYPE    = 5;
    private final static int BIN_TYPE    = 6;

    private final static int STRING_L_TYPE = -1;
    private final static int LONG_L_TYPE   = -2;
    private final static int INT_L_TYPE    = -3;
    private final static int DATE_L_TYPE   = -4;
    private final static int OID_L_TYPE    = -5;
    private final static int BIN_L_TYPE    = -6;



    // # of DBObjects to put in bulkops buffer before calling execute()
    private final static int DEFAULT_LISTSIZE = 16;

    private void reportError(String s, boolean forceExit) {
	reportError(s);
	System.exit(1);
    }

    private void reportError(String s) {
	p(s);
	if((boolean)params.get("stopOnError")) {
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
	p("importFile is a CR-delimited JSON or CSV file just like mongoimport would consume.");
	p("File will be divided down by numThreads and a starting region assigned to each thread.");
	p("Each region will be processed by a thread, parsing the CR-delimited JSON and adding");
	p("the DBObject to a bulk operations buffer.  When the listSize is reached, the bulk insert is executed.");
	p("Smallish document sizes (~256 bytes) will benefit from larger listSize");
	p("Default numThreads is 1 and default listSize is " + DEFAULT_LISTSIZE);
	p("");
	p("If only one importFile is specified and it names a directory, then ALL files in");
	p("that directory will be processed.  All options will apply to all the files");
	p("processed in that directory");

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
	p("--parseOnly          perform all parsing actions but do NOT effect the target DB (no connect, no drop, no insert)");
	p("--drop            drop collection before inserting");
	p("--type json|csv   identify type of file (json is default)");
	p("--threads n       number of threads amongst which to divide the read/parse/insert logic");
	p("--listSize n      size of bulkOperations buffer");


	// FIELDS
	p("");
	p("--fieldPrefix str    (CSV only) If --fields not present OR number of items on current line > spec in");
	p("                     --fields, then name the field str%d where %d is the zero-based index of the item");
	p("--fields spec    (CSV only) spec is fldName[:fldType[:fldFmt]][, ...]");
	p("                 fldType is optional and is string by default");
	p("                 fldType one of string, int, long, date, oid, binary00");
	p("                 OR above with List appended e.g. stringList");
	p("                 e.g. --fields 'name,age:int,bday:date:YYYYMMDD'");
	p("                 Each item on line will be named fldName and the string value converted to the");
	p("                 specified fldType.  *List types are special; items in the line must be quoted");
	p("                 delimited and will be further split and the result assigned to an array.  Given:");
	p("                        steve,\"dog,cat\"  ");
	p("                 --fields \"name,pets\"  will produce { name: steve, pets: \"dog,cat\"} ");
	p("                 --fields \"name,pets:stringList\" will produce { name: steve, pets: [ \"dog\", \"cat\"] }");
	p("                 fldType date formats: YYYY-MM-DD (default), YYYYMMDD, YYMMDD (1970 cutoff), YYYYMM (assume day 1)");

	//  HANDLER!
	p("");
	p("--handler classname      load a class using standard classloader semantics that implements MongoImportHandler:");
	p("                        public interface MongoImportHandler {");
	p("                          void init();");
	p("                          boolean process(java.util.Map);");
	p("                          void done();");   
	p("                        }");
	p("                        init() is called once at the start of each file to be processed.  ");
	p("                        process(Map) is called with the parsed Map (post-mongoDB JSON type convention conversion)");
	p("                        and any operation can be performed on the map.  If process() returns false the map ");
	p("                        will not be inserted.");
	p("                        done() is called once following processing of all items.");
	p("                         handler is called by all threads so take care to add synchronization where necessary.");



	//  PARSER!
	p("");
	p("--parser classname      load a class using standard classloader semantics that implements MongoImportParser:");
	p("                        public interface MongoImportParser {");
	p("                          void init();");
	p("                          boolean process(String, java.util.Map);");
	p("                          void done();");   
	p("                        }");
	p("                        init() is called once at the start of each file to be processed.  ");
	p("                        process(String, Map) is called with the UNparsed line of data read from the file and ");
	p("                        an empty Map.  Any logic can be performed to load name-value content into the map.");
	p("                        If process() returns false this signals the parser failed to process the line of data.");
	p("                        done() is called once following processing of all items.");
	p("                        parser is called by all threads so take care to add synchronization where necessary.");
	p("                        parser overrides the --type option.");



	p("--verbose        chatty output");
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
		
		FileReader fr = new FileReader(this.fname);
		BufferedReader buffrd = new BufferedReader(fr);
		
		BulkWriteOperation bo = null;

		// level 0 is what mongoimport uses...?
		/** 
		 *  Testing with and without WriteConcern reveals on my MBP
		 *  that the perf is the same.
		 *  So do not use it for now...
		 */
		//WriteConcern wc = new WriteConcern(0); 

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

		    if(fileType == CSV_TYPE) {
			CSVReader csvr = null;

			csvr = new CSVReader(new StringReader(s));

			String[] toks = csvr.readNext();
			if(toks != null) {
			    mm = new HashMap(); 
			    for(int jj = 0; jj < toks.length; jj++) {
				if(toks[jj].length() == 0) {
				    continue;
				}
				if(ftypes != null && jj < maxFtypes) {
				    FieldType ft = ftypes.get(jj);
				    Object o = processItem(ft, toks[jj]);
				    if(o != null) {
					mm.put(ft.name, o);
				    }
				} else {
				    mm.put(fldpfx + jj, toks[jj]);
				}
			    }
			}
			
		    } else if(fileType == JSON_TYPE) {
			try {
			    JsonParser2 p = new JsonParser2(s);
			    Object value = p.parse(true);
			    if(value == null) {
				reportError("error: content " + s + " parses to null");
			    }
			    mm = (Map<String,Object>) value;

			} catch(Exception e) {
			    reportError("error: content " + s + ": " + e);
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
				if(0 == numRows % listsize) {
				    BulkWriteResult rc = bo.execute();
				    rr += rc.getInsertedCount();
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
		}


		if(bo != null) {
		    BulkWriteResult rc = bo.execute();
		    rr += rc.getInsertedCount();
		    bo = null;
		}


		buffrd.close();

		pp(this.tnum + " wrote " + rr + " of " + numRows);

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

	    listsize = DEFAULT_LISTSIZE;

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

		} else if(a.equals("--listSize")) {
		    j++;
		    listsize = Integer.parseInt(args[j]);

		} else if(a.equals("--drop")) {
		    params.put("drop", true);

		} else if(a.equals("--verbose")) {
		    params.put("verbose", true);

		} else if(a.equals("--parseOnly")) {
		    params.put("parseOnly", true);

		} else if(a.equals("--fields")) {
		    j++;
		    params.put("fields", args[j]);

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

		} else if(a.equals("--type")) {
		    j++;
		    String s = args[j].toLowerCase();
		    if(s.equals("json")) {
			params.put("type", JSON_TYPE);
		    } else if(s.equals("csv")) {
			params.put("type", CSV_TYPE);
		    }

		} else if(a.equals("--help") || a.equals("-help")) {
		    usage();
		    System.exit(0);

		} else {
		    // Not a -- arg; must be an importfile
		    files.add(a);
		}
	    }


	    if(! (boolean) params.get("parseOnly")) {
		MongoClient mongoClient = null;

		ServerAddress sa = new ServerAddress((String)params.get("host"),(Integer)params.get("port"));

		String dbname = (String)params.get("db");
		
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
			    reportError("error: name+password supplied not valid for database " + dbname, true);
			}
		    } else {
			mongoClient = new MongoClient(sa);
		    }
		}

		DB db = mongoClient.getDB(dbname);
		coll = db.getCollection( (String)params.get("coll") );

		
		if((boolean)params.get("drop")) {
		    pp("dropping " + (String)params.get("coll"));
		    coll.drop();
		}
	    } else {
		pp("parseOnly mode; no database actions will be taken");
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

	    ftypes = null;
	    if(fileType == CSV_TYPE) {
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



	    for(int kk = 0; kk < files.size(); kk++) {
		pp("working on " + files.get(kk));
		if(parser != null) { parser.init(); }
		if(handler != null) { handler.init(); }
		totProc += processFile(files.get(kk), nthreads);
		if(handler != null) { handler.done(); }
		if(parser != null) { parser.done(); }
	    }

	    java.util.Date end = new java.util.Date();
	    long diff = end.getTime() - start.getTime();

	    pp(totProc + " items; millis: " + diff);
	    double psec = ((double)totProc / ((double)diff / 1000));
	    pp("performance " + psec);

	} catch (Exception e) {
	    System.out.println("go() epic fail: " + e);
	    e.printStackTrace();
	}
    }


    private static Object processItem(FieldType ft, String input) 
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
	    ox = Integer.parseInt(input);
	    break;

	case LONG_TYPE:
	    ox = Long.parseLong(input);
	    break;

	case OID_TYPE:
	    break;

	case BIN_TYPE: {
	    byte[] vv = javax.xml.bind.DatatypeConverter.parseBase64Binary(input); 	    // YOW!
	    ox = vv;
	    break;
	}

	case STRING_L_TYPE: { // optimized performance here...
	    CSVReader xx = new CSVReader(new StringReader(input));
	    String[] vv = xx.readNext();
	    ox = vv;
	    break;
	}

	case INT_L_TYPE: 
	case LONG_L_TYPE: 
	case DATE_L_TYPE:
	case BIN_L_TYPE:
	{
	    CSVReader xx = new CSVReader(new StringReader(input));
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

    private static List<FieldType> processFields(String ff) {
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
		}

		fl.add(new FieldType(fn, tt, fm));
	    }
	} catch(Exception e) {
	    System.out.println("processField() epic fail: " + e);
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
	    pp("running " + nthreads + " threads, listsize " + listsize);
	    
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




    // buildNumber(s, 0, 4);
    private static int buildNumber(String s, int idx, int len) {
	int item = 0;

	// k is going 0 to len-1. i.e. 0 to 3 for year...
	for(int k = 0; k < len; k++) {

	    // Given 2013, jump to 3 
	    int nx = idx + (len-1) - k;

	    char c = s.charAt(nx);
	    switch(k) {
	    case 3:
		item += (c - '0') * 1000;
		break;
	    case 2:
		item += (c - '0') * 100;
		break;
	    case 1:
		item += (c - '0') * 10;
		break;
	    case 0:
		item += (c - '0');
		break;
	    }
	}

	return item;
    }

    private static Date cvtDate(String s, String fmt) {

	Date d = null;

	if(fmt == null || fmt.equals("YYYY-MM-DD")) {
	    int year  = buildNumber(s, 0, 4);
	    int month = buildNumber(s, 5, 2);
	    int day   = buildNumber(s, 8, 2);
	    Calendar cal = Calendar.getInstance();
	    cal.clear();
	    cal.set( Calendar.YEAR,  year );
	    cal.set( Calendar.MONTH, month - 1);
	    cal.set( Calendar.DATE,  day ); 
	    d = cal.getTime();					

	} else if(fmt.equals("YYYYMMDD")) {
	    int year  = buildNumber(s, 0, 4);
	    int month = buildNumber(s, 4, 2);
	    int day   = buildNumber(s, 6, 2);
	    Calendar cal = Calendar.getInstance();
	    cal.clear();
	    cal.set( Calendar.YEAR,  year );
	    cal.set( Calendar.MONTH, month - 1);
	    cal.set( Calendar.DATE,  day ); 
	    d = cal.getTime();					

	} else if(fmt.equals("YYMMDD")) {
	    int year  = buildNumber(s, 0, 2);
	    if(year < 70) { 
		year += 2000; // 140203, 650203 -> 3-Feb-2014, 3-Feb-2065
	    } else { 
		year += 1900; // 700203, 960203 -> 3-Feb-2070, 3-Feb-1996
	    }
	    int month = buildNumber(s, 2, 2);
	    int day   = buildNumber(s, 4, 2);
	    Calendar cal = Calendar.getInstance();
	    cal.clear();
	    cal.set( Calendar.YEAR,  year );
	    cal.set( Calendar.MONTH, month - 1);
	    cal.set( Calendar.DATE,  day ); 
	    d = cal.getTime();					

	} else if(fmt.equals("YYYYMM")) { // 201405 becomes 1-May-2014
	    int year  = buildNumber(s, 0, 4);
	    int month = buildNumber(s, 4, 2);
	    int day   = 1; // YOW!
	    Calendar cal = Calendar.getInstance();
	    cal.clear();
	    cal.set( Calendar.YEAR,  year );
	    cal.set( Calendar.MONTH, month - 1);
	    cal.set( Calendar.DATE,  day ); 
	    d = cal.getTime();					

	} else if(fmt.equals("YYYY-MM-DDTHH:MM:SS.SSS-HHMM")) {
	    d = cvtMongoStrDateToDate(s);
	}

	return d;
    }

    /**
     *  2014-06-23T09:26:26.214-0400
     *  0123456789012345678901234567890
     *            1         2     
     */
    private static Date cvtMongoStrDateToDate(String s) {
	int year = 0;
	int month = 0;
	int day = 0;
	
	int hour = 0;
	int min = 0;
	int sec = 0;
	int msec = 0;
	int tzhrs = 0;
	int tzmin = 0;

	
	year  = buildNumber(s, 0, 4);
	month = buildNumber(s, 5, 2);
	day   = buildNumber(s, 8, 2);
	hour  = buildNumber(s, 11, 2);
	min   = buildNumber(s, 14, 2);
	sec   = buildNumber(s, 17, 2);
	msec = buildNumber(s, 20, 3);	

	tzhrs = buildNumber(s, 24, 2);
	tzmin = buildNumber(s, 26, 2);
	int mult = 1;
	if(s.charAt(23) == '+') {
	    mult = -1;
	}
	
	Calendar cal = Calendar.getInstance();
	cal.clear();
	cal.set( Calendar.YEAR,  year );
	cal.set( Calendar.MONTH, month - 1); // YOW!
	cal.set( Calendar.DATE,  day ); 
	cal.set( Calendar.HOUR_OF_DAY, hour ); 
	cal.set( Calendar.MINUTE, min );
	cal.set( Calendar.SECOND, sec );
	cal.set( Calendar.MILLISECOND, msec ); 	    

	// Still kinda unsure about all this...
	//cal.add( Calendar.HOUR_OF_DAY, tzhrs * mult);
	//cal.add( Calendar.MINUTE, tzmin * mult);

	Date d = cal.getTime();					

	return d;
    }
}
