import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {

	private static void waitFor(ArrayList<Process> processes) {
		for (Process p : processes) {
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FileInputStream fi = null;
		Scanner sc = null;
		ArrayList<Process> processes = new ArrayList<Process>();
		try {
			fi = new FileInputStream("/cal/homes/mgaby/Desktop/servers.txt");
			sc = new Scanner(fi);
			while (sc.hasNextLine()) {
				String serverName = sc.nextLine();
				try {
					String userName = "mgaby@";
					// command should look something like this: 
					// ssh mgaby@tp-4b01-02 mkdir -p /tmp/mgaby && scp /tmp/mgaby/SLAVE.jar mgaby@tp-4b01-02:/tmp/mgaby/SLAVE.jar
					ProcessBuilder pb = new ProcessBuilder("ssh", userName + serverName, "rm", "-rf", "/tmp/mgaby/");
					Process p = null;
					File log = new File("/tmp/mgaby/clean.log");
					try {
						log.createNewFile();
					} catch (Exception e) {
						e.printStackTrace();
					}
					File error = new File("/tmp/mgaby/clean.err");
					try {
						error.createNewFile();
					} catch (Exception e) {
						e.printStackTrace();
					}
					pb.redirectOutput(Redirect.appendTo(log));
					pb.redirectError(Redirect.appendTo(error));
					p = pb.start();
					processes.add(p);
					assert pb.redirectOutput().file() == log;
					assert pb.redirectError().file() == error;
				} catch (Exception e) { e.printStackTrace(); }
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				sc.close();
			} catch(Exception e) { e.printStackTrace(); }
			try {
				fi.close();
			} catch(Exception e) { e.printStackTrace(); }
		}
		waitFor(processes);
		System.out.println("CLEAN FINISHED");
	}

}
