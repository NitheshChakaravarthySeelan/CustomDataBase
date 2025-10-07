package com.minidb.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Tokenizer {

    public enum TokenType {
        KEYWORD, IDENTIFIER, STRING_LITERAL, NUMERIC_LITERAL, SYMBOL, EOF
    }

    private static final Map<String, TokenType> KEYWORDS = Stream.of(new Object[][] {
        {"INSERT", TokenType.KEYWORD}, {"INTO", TokenType.KEYWORD}, {"VALUES", TokenType.KEYWORD},
        {"SELECT", TokenType.KEYWORD}, {"FROM", TokenType.KEYWORD}, {"WHERE", TokenType.KEYWORD},
        {"DELETE", TokenType.KEYWORD}, {"BETWEEN", TokenType.KEYWORD}, {"AND", TokenType.KEYWORD}
    }).collect(Collectors.toMap(data -> (String)data[0], data -> (TokenType)data[1]));

    private static final Map<Character, TokenType> SYMBOLS = Stream.of(new Object[][] {
        {'(', TokenType.SYMBOL}, {')', TokenType.SYMBOL}, {',', TokenType.SYMBOL},
        {';', TokenType.SYMBOL}, {'=', TokenType.SYMBOL}, {'*', TokenType.SYMBOL}
    }).collect(Collectors.toMap(data -> (Character)data[0], data -> (TokenType)data[1]));

    public String input = "";
    private int pos;

    public Tokenizer(String input) {
        this.input = input;
        this.pos = 0;
    }

    public List<Token> tokenize() throws ParseException {
        List<Token> tokens = new ArrayList<>();
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }
            if (c == '-' && pos + 1 < input.length() && input.charAt(pos + 1) == '-') {
                readComment();
                continue;
            }
            if (Character.isLetter(c)) {
                tokens.add(readIdentifierOrKeyword());
                continue;
            }
            if (c == '\'') {
                tokens.add(readStringLiteral());
                continue;
            }
            if (Character.isDigit(c)) {
                tokens.add(readNumericLiteral());
                continue;
            }
            if (SYMBOLS.containsKey(c)) {
                tokens.add(new Token(SYMBOLS.get(c), String.valueOf(c), pos++));
                continue;
            }
            throw new ParseException("Unexpected character: " + c + " at position " + pos);
        }
        tokens.add(new Token(TokenType.EOF, "", pos));
        return tokens;
    }

    private void readComment() {
        while (pos < input.length() && input.charAt(pos) != '\n') {
            pos++;
        }
    }

    private Token readIdentifierOrKeyword() {
        int start = pos;
        String identifier = readWhile(c -> Character.isLetterOrDigit(c) || c == '_');
        String upperCaseIdentifier = identifier.toUpperCase();
        if (KEYWORDS.containsKey(upperCaseIdentifier)) {
            return new Token(KEYWORDS.get(upperCaseIdentifier), upperCaseIdentifier, start);
        }
        return new Token(TokenType.IDENTIFIER, identifier, start);
    }

    private Token readStringLiteral() throws ParseException {
        int start = pos;
        pos++; // Skip opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != '\'') {
            if (input.charAt(pos) == '\'' && pos + 1 < input.length() && input.charAt(pos + 1) == '\'') {
                sb.append('\'');
                pos += 2;
            } else {
                sb.append(input.charAt(pos));
                pos++;
            }
        }
        if (pos >= input.length() || input.charAt(pos) != '\'') {
            throw new ParseException("Unclosed string literal starting at position " + start);
        }
        pos++; // Skip closing quote
        return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
    }
    
    private Token readNumericLiteral() {
        int start = pos;
        String number = readWhile(Character::isDigit);
        return new Token(TokenType.NUMERIC_LITERAL, number, start);
    }

    private String readWhile(Predicate<Character> predicate){
        int start = pos;
        while (pos < input.length() && predicate.test(input.charAt(pos))){
            pos++;
        }
        return input.substring(start, pos);
    }
}