import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

public class Main {

	@SuppressWarnings("deprecation")
	
	private static ArrayList<String> getServers() {
		final File SERVERS_FILE = new File("/tmp/mgaby/servers.txt");
		ArrayList<String> servers = new ArrayList<String>();
		Scanner serversScanner = null;
		try { 
			serversScanner = new Scanner(SERVERS_FILE);
			while (serversScanner.hasNext()) {
				servers.add(serversScanner.next());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				serversScanner.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return servers;
	}
	
	public static void waitForProcesses(ArrayList<Process> processes) {
		for (Process p : processes) {
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static ArrayList<String> getFiles(String fullDirectoryPath) {
		ArrayList<String> orderedFiles = new ArrayList<String>();
		File directory = new File(fullDirectoryPath);
		String[] files = directory.list();
		
		for (int i = 0; i < files.length; i++) {
			orderedFiles.add(files[i]);
		}
		Collections.sort(orderedFiles);
		return orderedFiles;
	}
	
	public static void map(ArrayList<String> servers) {
		ArrayList<Process> processes = new ArrayList<Process>();
		File log = new File("/tmp/mgaby/map.log");
		try {
			log.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File error = new File("/tmp/mgaby/map.error");
		try {
			error.createNewFile();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String userName = "mgaby";
		String hostname = getHostname();
		
		ArrayList<String> splits = getFiles("/tmp/mgaby/splits");
		for (int i = 0; i < splits.size(); i++) {
			String split = null;
			try {
				split = splits.get(i);
			} catch (Exception e) {
				break;
			}
			
			String serverName = servers.get(i % servers.size());
			try {
				ProcessBuilder pb = new ProcessBuilder("ssh", userName + "@" + serverName, "mkdir", "-p", "/tmp/" + userName + "/splits", "&&", "scp", userName + "@" + hostname + ":/tmp/" + userName + "/splits/" + split, userName+"@"+serverName+":/tmp/mgaby/splits/", "&&", "scp", userName + "@" + hostname + ":/tmp/mgaby/servers.txt", userName + "@" + serverName + ":/tmp/" + userName + "/", "&&", "java", "-jar", "/tmp/" + userName + "/SLAVE.jar", "0");
				pb.redirectOutput(Redirect.appendTo(log));
				pb.redirectError(Redirect.appendTo(error));
				Process p = pb.start();
				processes.add(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		waitForProcesses(processes);
		System.out.println("MAPPING FINISHED");
	}
	
	private static String getHostname() {
		ProcessBuilder pb = new ProcessBuilder("hostname");
		String hostname = null;
		try {
			Process p = pb.start();
			InputStream is = p.getInputStream();
			Scanner sc = new Scanner(is);
			p.waitFor();
			hostname = sc.next();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return hostname;
	}
	
	public static void shuffle(ArrayList<String> servers) {
		ArrayList<Process> processes = new ArrayList<Process>();
		File log = new File("/tmp/mgaby/shuffle.log");
		try {
			log.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File error = new File("/tmp/mgaby/shuffle.error");
		try {
			error.createNewFile();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String userName = "mgaby";
		String hostname = getHostname();
		for (int i = 0; i < servers.size(); i++) {
			try {
				ProcessBuilder pb = new ProcessBuilder("ssh", userName + "@" + servers.get(i), "java", "-jar", "/tmp/mgaby/SLAVE.jar 1");
				pb.redirectOutput(Redirect.appendTo(log));
				pb.redirectError(Redirect.appendTo(error));
				Process p = pb.start();
				processes.add(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		waitForProcesses(processes);
		System.out.println("SHUFFLING FINISHED");
	}
	
	public static void reduce(ArrayList<String> servers) {
		ArrayList<Process> processes = new ArrayList<Process>();
		File log = new File("/tmp/mgaby/reduces.log");
		try {
			log.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File error = new File("/tmp/mgaby/reduces.error");
		try {
			error.createNewFile();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String userName = "mgaby";
		String hostname = getHostname();
		for (int i = 0; i < servers.size(); i++) {
			try {
				ProcessBuilder pb = new ProcessBuilder("ssh", userName + "@" + servers.get(i), "java", "-jar", "/tmp/mgaby/SLAVE.jar 2");
				pb.redirectOutput(Redirect.appendTo(log));
				pb.redirectError(Redirect.appendTo(error));
				Process p = pb.start();
				processes.add(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		waitForProcesses(processes);
		System.out.println("REDUCE FINISHED");
	}
	
	private static void createSplits(String inputFileName) {
		File splitsDirectory = new File("/tmp/mgaby/splits");
		splitsDirectory.mkdir();
		File inputFile = new File(inputFileName);
		Scanner sc = null;

		try {
			sc = new Scanner(inputFile);
			int count = 0;
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				String newFileName = "/tmp/mgaby/splits/" + "S" + count + ".txt";
				count++;
				File newFile = new File(newFileName);
				newFile.createNewFile();
				PrintWriter pw = null;
				try {
					pw = new PrintWriter(newFile);
					pw.println(line);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					pw.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				sc.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void splits(String inputFileName) {
		File inputFile = new File(inputFileName);
		if (inputFile.exists()) {
			if (inputFile.isDirectory()) {
				String[] inputFileNames = inputFile.list();
				for (int i = 0; i < inputFileNames.length; i++) {
					createSplits("/tmp/mgaby/splits/" + inputFileNames[i]);
				}
			} else {
				createSplits(inputFileName);
			}
		}
	}
	
	private static void printResults(ArrayList<String> servers) {
		File log = new File("/tmp/mgaby/resultprint.log");
		try {
			log.delete();
			log.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File error = new File("/tmp/mgaby/resultprint.error");
		try {
			error.delete();
			error.createNewFile();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ArrayList<Process> processes = new ArrayList<Process>();
		for (String serverName : servers) {
			ProcessBuilder pb = new ProcessBuilder("ssh", "mgaby@" + serverName, "find", "/tmp/mgaby/reduces/", "-type", "f", "-exec", "cat", "{}", "+");
			pb.redirectOutput(Redirect.appendTo(log));
			pb.redirectError(Redirect.appendTo(error));
			try {
				Process p = pb.start();
				processes.add(p);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		waitForProcesses(processes);
		System.out.println("PRINTING FINISHED");
	}
	
	public static void printWordCount(Map<String, Integer> map) {
		System.out.println("Results:");
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
		    System.out.println(entry.getKey() + " = " + entry.getValue());
		}
	}
	
	private static void readSorted() {
		File resultsfile = new File("/tmp/mgaby/resultprint.log");
		Scanner sc = null;
		HashMap<String, Integer> map  = new HashMap<String, Integer>();
		try {
			sc = new Scanner(resultsfile);
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				String[] keyValuePair = line.split(" ");
				String key = keyValuePair[0];
				Integer val = Integer.parseInt(keyValuePair[1]);
				map.put(key,val);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		List<Map.Entry<String, Integer>> l  = new ArrayList<Map.Entry<String, Integer>>();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
		    l.add(entry);
		}
		
		Collections.sort(l, new ValueComparator());
		Collections.reverse(l);
		
		Map<String, Integer> valueSortedWordCount = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : l) {
			valueSortedWordCount.put(entry.getKey(), entry.getValue());
		}
		
		printWordCount(valueSortedWordCount);
	}
	
	private static void run() {
		final ArrayList<String> servers = getServers();
		long startTime = 0;
		long endTime   = 0;
		long totalTime = 0;
		
		startTime = System.currentTimeMillis();
		map(servers);
		endTime   = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println("MAPPING DURATION : " + totalTime);
		
		startTime = System.currentTimeMillis();
		shuffle(servers);
		endTime   = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println("SHUFFLING DURATION : " + totalTime);
		
		startTime = System.currentTimeMillis();
		reduce(servers);
		endTime   = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println("REDUCE DURATION : " + totalTime);
		
		printResults(servers);
		readSorted();
	}

	
	public static void main(String[] args) {
		if (args.length == 0) {
			String demoFileName = "/tmp/mgaby/demo.txt";
			splits(demoFileName);
			run();
		} else if (args.length == 1) {
			String inputFileName = args[0];
			splits(inputFileName);
			run();
		}
	}
}
