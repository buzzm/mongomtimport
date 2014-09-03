mongomtimport
=============

Multithreaded Java file loader for mongoDB

Basic use:

```
ant dist
java -cp mongo-java-driver-2.12.0.jar:dist/lib/mongoapps.jar mongomtimport --help
```

The current help line:

```
usage: mongomtimport -d db -c collection [ options ] importFile [ importfile ... ]
importFile is a CR-delimited JSON, delimited, or fixed-width file.
File will be divided down by n threads and a starting region assigned to each thread.
Each region will be processed by a thread, parsing the CR-delimited JSON and adding
the DBObject to a bulk operations buffer.  When the bulksize is reached, the bulk insert is executed.
Smallish document sizes (~1K bytes) will benefit from larger bulksize
Default threads is 1 and default bulksize is 16

If only one importFile is specified and it names a directory, then ALL files in
that directory will be processed.  All options will apply to all the files
processed in that directory

options:
--host | -h     name of host e.g. machine.firm.com (default localhost
--port          port number (default 27017)
--username | -u    user name (if auth required for DB)
--password | -p    password (if auth required for DB)

-d dbname       name of DB to load (default test)
-c collname     name of collection in DB to load (default test)

--parseOnly       perform all parsing actions but do NOT effect the target DB (no connect, no drop, no insert)
--drop            drop collection before inserting
--type json|delim|fixed   identify type of file (json is default)
--separator c     char to use as field and embedded list (see *List types) (default is comma)
--trim            (fixed and delim only) post-parse, strip leading and trailing whitespace from items.
                  If the field is trimmed to size 0, it will not be added.
                  Is a noop if a custom parser is in use.
                  Highly recommended for fixed length imports.
--threads n       number of threads amongst which to divide the read/parse/insert logic
--bulksize n      size of bulkOperations buffer for each thread
--stopOnError     do not try to continue if parsing/insert error occurs

--fieldPrefix str    (noop for JSON) If --fields not present OR number of items on current line > spec in
                     --fields, then name the field str%d where %d is the zero-based index of the item

--fields spec    (noop for JSON) spec is fldName[:fldType[:fldFmt]][, ...]
                 fldType is optional and is string by default
                 fldType one of string, int, long, double, date, binary00
                 OR above with List appended e.g. stringList
                 e.g. --fields 'name,age:int,bday:date:YYYYMMDD'
                 Each item on line will be named fldName and the string value converted to the
                 specified fldType.  *List types are special; items in the line must be quoted
                 delimited and will be further split and the result assigned to an array.  So given:
                        steve,"dog,cat"  
                   --fields "name,pets"  will produce { name: steve, pets: "dog,cat"} 
                 but
                   --fields "name,pets:stringList" will produce { name: steve, pets: [ "dog", "cat"] }
                 fldType date formats: YYYY-MM-DD (default), YYYYMMDD, YYMMDD (<70 add 2000 else add 1900), YYYYMM (assume day 1), ISO8601
                 ISO8601 format accepts milliseconds.  If timezone is not explicitly declared
                 using [+-]HHMM or Z (Z means +0000) then the timezone of the running process will be
                 taken into account

--fieldColumns   (fixed type only and required) A comma separated list of start and end position pairs where 1 (not 0) is the first column.
                 Dash (e.g. 3-6) defines start and end position, inclusive
                 Plus (e.g. 3+4) defines start position and length
                 Single columns may be represented without the dash or plus.
                     --fieldColumns 3-5,6-9,24,30-45
                 is equivalent to
                     --fieldColumns 3+3,6+4,24,30+16
                 Use --fields to name and type extracted columns otherwise default behavior is same as delimited file.
                 *List fields will use the separator character to further break up extracted columns

--handler classname      load a class using standard classloader semantics that implements MongoImportHandler:
                        public interface MongoImportHandler {
                          void init(String fileName);
                          boolean process(java.util.Map);
                          void done(String fileName);
                        }
                        init() is called once at the start of each file to be processed.  
                        process() is called with the parsed Map (post-mongoDB JSON type convention conversion)
                        and any operation can be performed on the map.  If process() returns false the map 
                        will not be inserted.
                        done() is called once following processing of all items.
                         handler is called by all threads so take care to add synchronization where necessary.

--parser classname      load a class using standard classloader semantics that implements MongoImportParser:
                        public interface MongoImportParser {
                          void init(String fileName);
                          boolean process(String, java.util.Map);
                          void done(String fileName);
                        }
                        init() is called once at the start of each file to be processed.  
                        process() is called with the UNparsed line of data read from the file and 
                        an empty Map.  Any logic can be performed to load name-value content into the map.
                        If process() returns false this signals the parser failed to process the line of data.
                        done() is called once following processing of all items.
                        parser is called by all threads so take care to add synchronization where necessary.
                        parser overrides the --type option.
--verbose        chatty output
```

To create your own handler or parser, make sure MongoImportHandler.java or
MongoImportParser.java (or the .class equivs either as files OR in a jar)
is visible to javac.   The mongoapps.jar contains the class files for both
so it is not necessary to repackage them into your own jar.
```
$ ls MongoImportHandler.java
MongoImportHandler.java
$ javac myHandler.java
$ jar cvf myHandlers.jar myHandler.class
$ java -cp myHandlers.jar:mongo-java-driver-2.12.0.jar:dist/lib/mongoapps.jar mongomtimport ...
```
