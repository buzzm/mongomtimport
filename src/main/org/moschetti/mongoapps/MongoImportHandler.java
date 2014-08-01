

public interface MongoImportHandler {
    /**
     *  Called once just before processing of file starts.
     *  This means in directory slurp mode or multifile input, init might be 
     *  called more than once, which is different from the constructor.
     */
    public void init();

    /**
     *  Knock yourself out.  
     *  The Map m comes populated from the parser.  You can look at anything
     *  inside, perform any logic you wish.  You can add or remove fields.
     *  What ends up in the Map upon exit goes into mongoDB!
     *  Return false to hold back the item, i.e. DON'T insert it.  Good
     *  if you wish to apply some filtering!
     */
    public boolean process(java.util.Map m);


    /**
     *  Called once just after processing of file starts.
     *  This means in directory slurp mode or multifile input, init might be 
     *  called more than once, which is different from the constructor.
     */
    public void done();
}