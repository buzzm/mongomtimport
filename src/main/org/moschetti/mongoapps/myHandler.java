
class myHandler implements MongoImportHandler {

    private long count;

    private synchronized long inc() {
	long x = count;
	count++;
	return count;
    }

    public void init() { count = 0; }

    public boolean process(java.util.Map m) {
	long x = inc();

	if(x == 1423) {
	    System.out.println("zing!");
	    m.put("hahah", "zing!");
	}
	return true;
    }

    public void done() {}
}