package com.minidb.raft;

/**
 * index:int Position in the log
 * term:int Term number when entry was received by leader
 * command:String The actual operation
 */
public class LogEntry {
	private int term;
	private String command;

	public LogEntry(int term, String command) {
		this.term = term;
		this.command = command;
	}

	public int getTerm() {
		return this.term;
	}

	public String getCommand() {
		return this.command;
	}

	public String toString() {
		String termString = Integer.toString(this.term);
		String result = "["+termString+", "+this.command+"]";
		return result;
	}
}
