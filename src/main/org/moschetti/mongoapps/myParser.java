
class myParser implements MongoImportParser {

    private long count;

    private synchronized long inc() {
	long x = count;
	count++;
	return count;
    }

    public void init(String fileName) { count = 0; }

    public boolean process(String s, java.util.Map m) {
	long x = inc();
	
	String[] flds = s.split(":");

	for(int i = 0; i < flds.length; i++) {
	    m.put("f" + i, flds[i]);
	}

	return true;
    }

    public void done(String fileName) {}
}