

public interface MongoImportHandler {
    /**
     *  Called once just before processing of file starts and is passed the
     *  name of the file.
     *  This means in directory slurp mode or multifile input, init might be 
     *  called more than once, which is different from the constructor.
     */
    public void init(String fileName);

    /**
     *  The Map m comes populated from the parser.  You can look at anything
     *  inside, perform any logic you wish.  You can add, remove, or modify
     *  entries in the Map.
     *  What ends up in the Map upon exit goes into mongoDB -- but be careful
     *  to only use types supported by mongoDB.
     *  Return false to hold back the item, i.e. DON'T insert it.  Good
     *  if you wish to apply some filtering!
     */
    public boolean process(java.util.Map m);


    /**
     *  Called once just after processing of file starts and is passed the
     *  name of the file.
     *  This means in directory slurp mode or multifile input, init might be 
     *  called more than once, which is different from the constructor.
     */
    public void done(String fileName);
}