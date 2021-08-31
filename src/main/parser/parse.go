package parser

import (
	"fmt"
	"strings"

	"cs/src/main/query"
)

func Parse(queryTxt string) (query.Query, error) {

	tokenizer := NewTokenizer(queryTxt)
	q, err := (&parser{tokenizer, stepType, query.Query{}, nil}).parse()
	return q, err
}

type step int

const (
	stepType step = iota

	stepSelectExpression
	stepSelectMainTable
	stepWhereExpression
	stepOrderbyExpression
	stepEnd
)

type parser struct {
	tokenizer *Tokenizer
	step      step
	query     query.Query
	err       error
}

func (p *parser) parse() (query.Query, error) {
	q, err := p.parseQuery()
	p.err = err
	return q, p.err
}

func (p *parser) parseQuery() (query.Query, error) {

	currentExpresion := query.Expression{}

	for {
		fmt.Printf("%n", p.step)
		switch p.step {
		case stepType:
			if strings.ToUpper(p.peekNextToken()) == "SELECT" {
				p.query.QueryType = query.Select
				p.step = stepSelectExpression
			} else {
				return p.query, fmt.Errorf("invalid query type")
			}
		case stepSelectExpression:

			token := p.peekNextToken()

			if strings.ToUpper(token) == "FROM" {
				p.step = stepSelectMainTable
				p.query.SelectExpressionsList = append(p.query.SelectExpressionsList, currentExpresion)
				currentExpresion = query.Expression{}
			} else if strings.ToUpper(token) == "," {
				p.query.SelectExpressionsList = append(p.query.SelectExpressionsList, currentExpresion)
				currentExpresion = query.Expression{}
			} else {
				query.AddValueToExpression(&currentExpresion, token)
			}
		case stepSelectMainTable:
			table := p.peekNextToken()

			if len(table) == 0 {
				return p.query, fmt.Errorf("expected table name")
			}
			p.query.Table = table
			token := p.peekNextToken()
			if strings.ToUpper(token) == "WHERE" {
				p.step = stepWhereExpression
			} else {
				p.step = stepEnd
			}
		case stepWhereExpression:
			token := p.peekNextToken()
			if strings.ToUpper(token) == "ORDER" {
				p.peekNextToken()
				p.step = stepOrderbyExpression
				p.query.WhereExpressionList = append(p.query.WhereExpressionList, currentExpresion)
				currentExpresion = query.Expression{}
			} else if strings.ToUpper(token) == "AND" {
				p.query.WhereExpressionList = append(p.query.WhereExpressionList, currentExpresion)
				currentExpresion = query.Expression{}
			} else if strings.ToUpper(token) == ";" {
				p.step = stepEnd
			} else {
				query.AddValueToExpression(&currentExpresion, token)
			}

		case stepOrderbyExpression:
			token := p.peekNextToken()
			if strings.ToUpper(token) == ";" {
				p.query.Orderby = append(p.query.Orderby, currentExpresion)
				p.step = stepEnd
			} else if strings.ToUpper(token) == "," {
				p.query.Orderby = append(p.query.Orderby, currentExpresion)
				currentExpresion = query.Expression{}
			} else {
				query.AddValueToExpression(&currentExpresion, token)

			}
		case stepEnd:
			//TODO update this part
			return p.query, nil

		}
	}
}

func (p *parser) peekNextToken() string {
	tk := p.tokenizer.nextToken()
	return tk
}
