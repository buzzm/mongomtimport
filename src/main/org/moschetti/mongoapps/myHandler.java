
class myHandler implements MongoImportHandler {

    private long count;

    private synchronized long inc() {
	count++;
	return count;
    }

    public void init(String fileName) { count = 0; }

    public boolean process(java.util.Map m) {
	long x = inc();

	if(x == 8) {
	    System.out.println("zing!");
	    m.put("hahah", "zing!");
	}
	return true;
    }

    public void done(String fileName) {}
}