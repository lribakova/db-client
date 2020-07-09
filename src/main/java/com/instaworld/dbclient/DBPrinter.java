package com.instaworld.dbclient;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class DBPrinter {

	private Connection connection;
	private static final String EXPORT_FILE = "result.csv";

	private DBPrinter(String dbUrl) {
		try {
			connection = DriverManager.getConnection(dbUrl);
			System.out.println("Connection established.");
		} catch (SQLException e) {
			throw new IllegalStateException("Cannot establish connection to DB");
		}
	}

	public static DBPrinter connect(String dbUrl) {
		return new DBPrinter(dbUrl);
	}

	private Connection getConnection() {
		return connection;
	}

	public void disconnect() {
		try {
			getConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void printTables() {
		try {
			DatabaseMetaData md = getConnection().getMetaData();
			ResultSet rs = md.getTables(null, null, "%", null);
			while (rs.next()) {
				System.out.println(rs.getString(3));
			}
		} catch (SQLException e) {
			System.out.println("Failed to show tables." + e.getMessage());
		}
	}

	public void executeUpdate(String query) {
		try (Statement st = getConnection().createStatement()) {
			int rows = st.executeUpdate(query);
			System.out.println(rows + " rows affected");
		} catch (SQLException e) {
			System.out.println("Wrong query.\n" + e.getMessage());
		}
	}

	public void execute(String query) {
		try (Statement st = getConnection().createStatement()) {
			st.execute(query);
		} catch (SQLException e) {
			System.out.println("Wrong query.\n" + e.getMessage());
		}
	}

	public void printSelect(String query) {
		try (Statement st = getConnection().createStatement(); ResultSet result = st.executeQuery(query)) {
			ResultSetMetaData rsmd = result.getMetaData();
			StringBuilder formattedStr = new StringBuilder("|");
			StringBuilder captionLine = new StringBuilder("|");

			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				int length = getLimitedMaximum(rsmd.getPrecision(i), rsmd.getColumnName(i).length(), 15) + 2;
				String formattedValue = " %-" + length + "." + length + "s |";
				formattedStr.append(formattedValue);
				String label = String.format(formattedValue, rsmd.getColumnName(i));
				captionLine.append(label);
			}

			printLine(captionLine.length());
			System.out.println(captionLine);
			printLine(captionLine.length());

			int rowCounter = 0;
			while (result.next()) {
				Object[] row = new Object[rsmd.getColumnCount()];
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					row[i - 1] = result.getObject(i);
				}
				System.out.printf(formattedStr.toString() + "\n", row);
				rowCounter++;
			}

			printLine(captionLine.length());
			System.out.println("(" + rowCounter + " rows)");
		} catch (SQLException e) {
			System.out.println("Wrong query.\n" + e.getMessage());
		}
	}

	private static int getLimitedMaximum(int... values) {
		Arrays.sort(values);
		int max = values[values.length - 1];
		return max > 80 ? 80 : max;
	}

	private void printLine(int amount) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			result.append("-");
		}
		System.out.println(result);
	}

	public void exportSelect(String query) {
		try (FileWriter fileWriter = new FileWriter(EXPORT_FILE);
				Statement st = getConnection().createStatement();
				ResultSet result = st.executeQuery(query)) {

			ResultSetMetaData rsmd = result.getMetaData();

			String[] columnNames = new String[rsmd.getColumnCount()];
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				columnNames[i - 1] = rsmd.getColumnName(i);
			}
			fileWriter.write(String.join(",", columnNames) + "\n");

			while (result.next()) {
				String[] row = new String[rsmd.getColumnCount()];
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					Object obj = result.getObject(i);
					row[i - 1] = obj == null ? "" : obj.toString();
				}
				fileWriter.write(String.join(",", row) + "\n");
			}
			System.out.println("Data was exported to " + EXPORT_FILE);

		} catch (SQLException e) {
			System.out.println("Wrong query.\n" + e.getMessage());
		} catch (IOException e) {
			System.out.println("Failed to create file.\n" + e.getMessage());
		}
	}
}
