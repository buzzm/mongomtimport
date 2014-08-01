

public interface MongoImportParser {
    /**
     *  Called once just before processing of file starts.
     *  This means in directory slurp mode or multifile input, init might be 
     *  called more than once, which is different from the constructor.
     */
    public void init();

    /**
     *  Knock yourself out.  
     *  The String s is a line from your input file.  It is your job to do whatever
     *  you wish to populate the given input Map m, which is allocated but empty.
     *  Return true if you did something successful.
     *  Return false if you could not parse the input.
     */
    public boolean process(String input, java.util.Map m);


    /**
     *  Called once just after processing of file starts.
     *  This means in directory slurp mode or multifile input, init might be 
     *  called more than once, which is different from the constructor.
     */
    public void done();
}