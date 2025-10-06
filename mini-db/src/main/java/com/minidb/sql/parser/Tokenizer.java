package com.minidb.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
/*
 * Walk input char by char
 * Skip whitespace and comments
 * If char is a letter, read an alpha-numeric token and norm, if it matches a keywork (case insensitive) return keyword token else identifier
 * If char is ': read until matching ', supporting doubled single quotes as excape
 * If char is a digit read number literal 
 * Single char tokens map directly 
 */
public class Tokenizer {

    public enum TokenType {
        KEYWORD, IDENTIFIER, OPERATOR, LITERAL, EOF
    }
    public enum Keyword {
        INSERT, INTO, VALUES, SELECT, FROM, WHERE, DELETE, BETWEEN, AND
    }
    public String input = "";
    private int pos;

    public Tokenizer(String input) {
        this.input = input;
        this.pos = 0;
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }
            if (c == '/' && pos + 1 < input.length() && input.charAt(pos + 1) == '/') {
                pos = readComment();
                continue;
            }
            if (Character.isLetter(c)) {
                tokens.add(readIdentifierOrKeyword());
                continue;
            }
            if (c == '\'' || c == '"') {
                tokens.add(new Token(TokenType.LITERAL, readString(c), pos));
                continue;
            }
            if (Character.isDigit(c)) {
                tokens.add(new Token(TokenType.LITERAL, readNumberLiteral(), pos));
                continue;
            }
            tokens.add(new Token(TokenType.OPERATOR, String.valueOf(c), pos));
            pos++;
        }
        tokens.add(new Token(TokenType.EOF, "", pos));
        return tokens;
    }
    private int readComment() {
        while (pos < input.length() && input.charAt(pos) != '\n') {
            pos++;
        }
        return pos;
    }   

    private Token readIdentifierOrKeyword() {
        String identifier = readWhile(Character::isLetterOrDigit);
        String keyword = identifier.toUpperCase();
        if (Keyword.valueOf(keyword) != null) {
            return new Token(TokenType.KEYWORD, keyword, pos);
        }
        return new Token(TokenType.IDENTIFIER, identifier, pos);
    }
    
    private String readNumberLiteral() {
        return readWhile(Character::isDigit);
    }

    private String readWhile(Predicate<Character> predicate){
        int start = pos;
        while (pos < input.length() && predicate.test(input.charAt(pos))){
            pos++;
        }
        return input.substring(start, pos);
    }

    private String readString(char quote) {
        pos++;
        int start = pos;
        while (pos < input.length() && input.charAt(pos) != quote) {
            pos++;
        }
        if (pos >= input.length()) {
            throw new RuntimeException("Unterminated string literal");
        }
        pos++;
        return input.substring(start, pos);

    }

}
