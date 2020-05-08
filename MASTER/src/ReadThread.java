import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;

public class ReadThread extends Thread {
	LinkedBlockingQueue<Integer> lbq;
	InputStream is;
	public ReadThread(LinkedBlockingQueue<Integer> lbq, InputStream is) {
		this.lbq = lbq;
		this.is = is;
	}
	
	@Override
	public void run() {
		String s;
		while(true) {
			try {
				// important to add get numeric value because the stream is considered to be a stream of char*
				// if not we will get the ascii value of the integer we read()
				lbq.put(Character.getNumericValue(is.read()));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
