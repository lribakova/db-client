package com.instaworld.dbclient;

import java.util.Scanner;

public class Main {

	private static final String RECONNECT = "reconnect";
	private static final String EXIT = "exit";

	public static void main(String[] args) {

		Scanner scanner = new Scanner(System.in);
		String command = null;

		while (!EXIT.equals(command)) {
			String dbFilePath;

			if (command == null && args.length == 1) {
				dbFilePath = args[0];
			} else {
				System.out.println("Enter path to db-file:");
				dbFilePath = scanner.nextLine();
			}

			String dbUrl = "jdbc:sqlite:" + dbFilePath;
			command = execute(dbUrl, scanner);
		}

		scanner.close();
	}

	private static String execute(String dbUrl, Scanner scanner) {
		DBPrinter db = DBPrinter.connect(dbUrl);

		String sqlCommand = null;
		while (!EXIT.equals(sqlCommand) && !RECONNECT.equals(sqlCommand)) {
			System.out.print("\ndb-client>");

			String query = scanner.nextLine();
			String[] splittedQuery = query.split(" ", 2);
			sqlCommand = splittedQuery[0].toLowerCase();

			switch (sqlCommand) {
			case "select":
				db.printSelect(query);
				break;
			case "alter":
			case "create":
			case "drop":
				db.execute(query);
				break;
			case "insert":
			case "update":
			case "delete":
				db.executeUpdate(query);
				break;
			case "show":
				db.printTables();
				break;
			case "export":
				db.exportSelect(splittedQuery[1]);
				break;
			case "":
				break;
			case RECONNECT:
				System.out.println();
				break;
			case EXIT:
				break;
			case "help":
			case "--help":
			case "-h":
				printHelp();
				break;
			default:
				System.out.println("Unsupported command.");
			}
		}

		db.disconnect();

		return sqlCommand;
	}

	private static void printHelp() {
		System.out.println();
		System.out.println("db-client is used to connect to db-files to execute query, if file doesn't exist, program creates it.\nThese are common db-client commands used in various situations:");

		System.out.println("   print data:");
		System.out.println("      select");

		System.out.println("   export data to file:");
		System.out.println("      export <select query>");

		System.out.println("   update data:");
		System.out.println("      alter, create, drop");
		System.out.println("      insert, update, delete");

		System.out.println("   print tables:");
		System.out.println("      show [tables]");

		System.out.println("   reconnect to another db-file:");
		System.out.println("      reconnect");

		System.out.println("   'help' list available subcommands");
	}
}
