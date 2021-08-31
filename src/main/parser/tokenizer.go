package parser

import (
	"fmt"
	"strings"
)

type Tokenizer struct {
	index     int
	lastChar  uint16
	LastToken string
	ST        string
}

const SYMBOLS = "[{}().,;+-*/&|<>=~]"

// Returns if character is digit
func isBlank(ch uint16) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}

// Returns if character is digit
func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}

// Returns if character is letter
func isLetter(ch uint16) bool {
	return 'A' <= ch && ch <= 'Z' || 'a' <= ch && ch <= 'z'
}

// Returns if character is letter
func isSymbol(ch uint16) bool {
	return strings.Contains(SYMBOLS, fmt.Sprintf("%v", ch))
}

// Main sql keywords
func isKeyword(st string) bool {
	st = strings.ToLower(st)
	return st == "select" || st == "where" || st == "inner" || st == "outer" || st == "join" || st == "groupby"
}

// moves to next character of tokenizr
func (tkn *Tokenizer) next() {
	tkn.index += 1
}

// top
func (tkn *Tokenizer) top() uint16 {
	if tkn.index >= len(tkn.ST) {
		return 0
	}
	return uint16(tkn.ST[tkn.index])
}

// peek
func (tkn *Tokenizer) peek() uint16 {
	if tkn.index+1 >= len(tkn.ST) {
		return 0
	}
	return uint16(tkn.ST[tkn.index+1])
}

// returns next token
func (tkn *Tokenizer) nextToken() string {
	curToken := ""

	// Skip over blank
	curChar := tkn.top()
	for isBlank(curChar) {
		tkn.next()
		curChar = tkn.top()
		if curChar == 0 {
			return curToken
		}
	}
	if isSymbol(curChar) {
		tkn.next()
		return fmt.Sprintf("%v", curChar)
	}
	for true {
		if curChar == 0 || isBlank(curChar) {
			return curToken
		}

		curToken += fmt.Sprintf("%v", curChar)

		nextChar := tkn.peek()
		if isBlank(nextChar) || isSymbol(nextChar) {
			tkn.next()
			return curToken
		}

		tkn.next()
		curChar = tkn.top()
	}

	return curToken
}

// returns new tokenizer
func NewTokenizer(st string) *Tokenizer {
	return &Tokenizer{index: 0, lastChar: 0, LastToken: "", ST: st}
}

func PrintTokens(st string) {
	tokenizer := NewTokenizer(st)
	for {
		tok := tokenizer.nextToken()
		if tok == "" {
			break
		}
		fmt.Printf("%s\n", tok)
	}
}
