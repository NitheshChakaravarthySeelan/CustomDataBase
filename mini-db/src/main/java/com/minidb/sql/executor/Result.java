package com.minidb.sql.executor;

import java.util.List;

public class Result {
    public final boolean ok;
    public final String errorMessage;
    public final List<Pair<String,String>> rows; // for select results; empty for writes

    private Result(boolean ok, String errorMessage, List<Pair<String,String>> rows) {
        this.ok = ok; this.errorMessage = errorMessage; this.rows = rows;
    }
    public static Result ok(List<Pair<String,String>> rows) { return new Result(true, null, rows); }
    public static Result ok() { return new Result(true, null, List.of()); }
    public static Result error(String msg) { return new Result(false, msg, List.of()); }
}