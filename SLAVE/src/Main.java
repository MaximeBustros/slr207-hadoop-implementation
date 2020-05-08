import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Scanner;
import java.util.ArrayList;

public class Main {

	private static void waitForProcesses(ArrayList<Process> processes) {
		for (Process p : processes) {
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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

	private static boolean createFolder(String name) {
		try {
			final String OUTPUT_FOLDER = "/tmp/mgaby/" + name;
			ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", OUTPUT_FOLDER);
			Process p = pb.start();
			p.waitFor();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static void map() {
		if (createFolder("maps")) {
			final String SPLITS_DIRECTORY = "/tmp/mgaby/splits/";
			final File SPLITS_FILES = new File(SPLITS_DIRECTORY);
			final String[] SPLITS_FILE_NAMES = SPLITS_FILES.list();
			// for each file in splices directory do the following
			for (int i = 0; i < SPLITS_FILE_NAMES.length; i++) {
				map("/tmp/mgaby/splits/" + SPLITS_FILE_NAMES[i]);
			}
			System.out.print("map was successful");
		} else {
			System.err.println("Could not create map folder");
		}
	}

	private static File createOutputFile(String filename) {
		try {
			final char FILE_NUMBER = filename.charAt(filename.indexOf(".txt") - 1);
			final String FILE_OUTPUT_NAME = "UM" + FILE_NUMBER + ".txt";
			final String OUTPUT_FOLDER = "/tmp/mgaby/maps/";

			// create outputFolder if doesn't exist
			File shuffleDirectory = new File(OUTPUT_FOLDER);
			shuffleDirectory.mkdir();
			final String OUTPUT_NAME = OUTPUT_FOLDER + FILE_OUTPUT_NAME;
			ProcessBuilder pb = new ProcessBuilder("touch", OUTPUT_NAME);
			Process p = pb.start();
			p.waitFor();
			File file = new File(OUTPUT_NAME);
			return file;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	private static void map(String inputfilename) {
		Scanner sc = null;
		PrintWriter printWriter = null;
		try {
			File outputfile = createOutputFile(inputfilename);
			if (outputfile != null) {
				File inputfile = new File(inputfilename);
				sc = new Scanner(inputfile);
				FileWriter fileWriter = new FileWriter(outputfile);
				printWriter = new PrintWriter(fileWriter);
				while (sc.hasNext()) {
					String val = sc.next();
					String str = val + " 1";
					printWriter.println(str);
				}
			} else {
				System.err.println("Error outputfile creation");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				printWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				sc.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void shufflePreparation(String inputname) {
		Scanner sc = null;
		PrintWriter printWriter = null;
		FileWriter fileWriter = null;

		String OUTPUT_DIRECTORY_NAME = "/tmp/mgaby/shuffles/";
		File shuffleDirectory = new File(OUTPUT_DIRECTORY_NAME);
		shuffleDirectory.mkdir();

		try {
			File inputfile = new File(inputname);
			sc = new Scanner(inputfile);
			while (sc.hasNextLine()) {
				try {
					String txt = sc.nextLine();
					String[] line = txt.split(" ");
					String key = line[0];
					String val = line[1];
					int hash = key.hashCode();
					String hostname = getHostname();
					String OUTPUT_FILE_NAME = hash + "-" + hostname + ".txt";
					String fileName = OUTPUT_DIRECTORY_NAME + OUTPUT_FILE_NAME;

					fileWriter = new FileWriter(fileName, true);
					printWriter = new PrintWriter(fileWriter);
					printWriter.println(txt);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						printWriter.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} // While end
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void sendShuffles(ArrayList<String> machines, ArrayList<Process> processes) {
		int numberOfMachines = machines.size();
		final String SHUFFLES_DIRECTORY_NAME = "/tmp/mgaby/shuffles/";
		final File SHUFFLES_DIRECTORY = new File(SHUFFLES_DIRECTORY_NAME);
		final String[] SHUFFLES_FILE_NAMES = SHUFFLES_DIRECTORY.list();
		
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

		for (String shuffleFileName : SHUFFLES_FILE_NAMES) {
			int hash = 0;
			try {
				System.out.println("Shuffle File Name = " + shuffleFileName);
				hash = Integer.parseInt(shuffleFileName.substring(0, shuffleFileName.substring(1).indexOf("-") + 1));
			} catch (Exception e) {
				e.printStackTrace();
			}
			int machineNumber = Math.abs(hash % numberOfMachines);
			String machineName = machines.get(machineNumber);
			String hostname = getHostname();
			final String userName = "mgaby@";
			System.out.println("username = " + userName + "machine name = " + machineName);
			System.out.println("hostname = " + hostname);
			ProcessBuilder pbSend = new ProcessBuilder("ssh", userName + machineName, "mkdir", "-p",
						"/tmp/mgaby/shufflesreceived", "&&", "scp",
						userName + hostname + ":/tmp/mgaby/shuffles/" + shuffleFileName,
						userName + machineName + ":/tmp/mgaby/shufflesreceived/");
			try {
				Process p = pbSend.start();
				pbSend.redirectOutput(Redirect.appendTo(log));
				pbSend.redirectError(Redirect.appendTo(error));
				processes.add(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static ArrayList<String> getOrderedMachineList() {
		final File SERVERS_FILE = new File("/tmp/mgaby/servers.txt");
		Scanner serversScanner = null;
		ArrayList<String> machines = new ArrayList<String>();

		try {
			serversScanner = new Scanner(SERVERS_FILE);
			while (serversScanner.hasNext()) {
				String machine = serversScanner.next();
				machines.add(machine);
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
		return machines;
	}

	private static void shuffleExecution(ArrayList<Process> processes) {
		ArrayList<String> machines = getOrderedMachineList();
		sendShuffles(machines, processes);
	}

	private static void shuffle(String inputname, ArrayList<Process> processes) {
		// write into /tmp/mgaby/shuffles
		shufflePreparation(inputname);

		// By now we can assume that it has written the files with the correct names and
		// we can proceed
		// to sending the files to the right machines
		shuffleExecution(processes);
	}

	private static void shuffle() {
		if (createFolder("shuffles")) {
			final String MAPS_DIRECTORY = "/tmp/mgaby/maps/";
			final File MAPS_FILES = new File(MAPS_DIRECTORY);
			final String[] MAPS_FILE_NAMES = MAPS_FILES.list();

			ArrayList<Process> processes = new ArrayList<Process>();
			// for each file in splits directory do the following
			for (int i = 0; i < MAPS_FILE_NAMES.length; i++) {
				shuffle("/tmp/mgaby/maps/" + MAPS_FILE_NAMES[i], processes);
			}

			waitForProcesses(processes);
			System.out.println("shuffle was successful");
		} else {
			System.err.println("Could not create map folder");
		}
	}

	private static void reduce(String inputFile) {
		String OUTPUT_FOLDER_NAME = "/tmp/mgaby/reduces";
		File OUTPUT_FOLDER = new File(OUTPUT_FOLDER_NAME);
		try {
			OUTPUT_FOLDER.mkdir();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		File SHUFFLESRECEIVED_FILE = new File(inputFile);
		String fileName = SHUFFLESRECEIVED_FILE.getName();
		String hash = fileName.substring(0, fileName.indexOf("-"));
		
		String outputFileName = "/tmp/mgaby/reduces/" + hash + ".txt";
		File outputFile = new File(outputFileName);
		try {
			outputFile.createNewFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Scanner shuffleScanner = null;
		Scanner receivedScanner = null;
		FileWriter fw = null;
		PrintWriter pw = null;
		try {
			shuffleScanner = new Scanner(SHUFFLESRECEIVED_FILE);
			int count = 0;
			String key = "";
			while (shuffleScanner.hasNextLine()) {
				String line = shuffleScanner.nextLine();
				String[] keyValuePair = line.split(" ");
				key = keyValuePair[0];
				System.out.println(key);
				count++;
			}
			System.out.println("Count = " + count);
			
			// Now that we have count
			if (outputFile.length() == 0) {
				System.out.println("outputFileName is " + outputFileName);
				fw = new FileWriter(outputFileName);
				pw = new PrintWriter(fw);
				pw.println(hash + " " + count);
			} else {
				receivedScanner = new Scanner(outputFile);
				String line = receivedScanner.nextLine();
				String[] keyValuePair = line.split(" ");
				int oldValue = Integer.parseInt(keyValuePair[1]);
				int newValue = oldValue + count;
				fw = new FileWriter(outputFileName);
				pw = new PrintWriter(fw);
				pw.println(key + " " + newValue);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				shuffleScanner.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				pw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void reduce() {
		String SHUFFLESRECEIVED_DIRECTORY_NAME = "/tmp/mgaby/shufflesreceived/";
		File SHUFFLESRECEIVED_DIRECTORY = new File(SHUFFLESRECEIVED_DIRECTORY_NAME);
		String[] SHUFFLESRECEIVED_FILES = SHUFFLESRECEIVED_DIRECTORY.list();
		for (int i = 0; i < SHUFFLESRECEIVED_FILES.length; i++) {
			reduce(SHUFFLESRECEIVED_DIRECTORY_NAME + SHUFFLESRECEIVED_FILES[i]);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length == 0) {
			System.out.println("No modes or files selected");
		} else if (args.length == 1) {
			try {
				final int MODE = Integer.parseInt(args[0]);
				if (MODE == 0) {
					map();
				} else if (MODE == 1)
					shuffle();
				else if (MODE == 2)
					reduce();
			} catch (NumberFormatException e) {
				System.out.println("Please insert a correct mode either \"0 or 1\"");
			}
		} else if (args.length == 2) {
			try {
				final int MODE = Integer.parseInt(args[0]);
				String fileinputname = args[1];
				if (MODE == 0)
					map(fileinputname);
				else if (MODE == 1)
					shuffle(fileinputname, new ArrayList<Process>());
				else if (MODE == 2)
					reduce(fileinputname);
			} catch (NumberFormatException e) {
				System.out.println("Please insert a correct mode either \"0 or 1\"");
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Too many arguments provided");
		}
	}
}
