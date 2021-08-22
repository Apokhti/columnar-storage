package parser

import (
	"fmt"
	"strings"

	"github.com/Apokhti/cs/src/main/query"
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
	stepAfterTable
	stepWhere
	stepWhereExpression
	stepOrderby
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
		switch p.step {
		case stepType:
			toReturn := p.handleType()
			if toReturn {
				return p.query, fmt.Errorf("WRONG TYPE")
			}
		case stepSelectExpression:

			token := p.peekNextToken()

			if strings.ToUpper(token) == "FROM" {
				p.step = stepSelectMainTable
				p.query.SelectExpressionsList = append(p.query.SelectExpressionsList, currentExpresion)
				currentExpresion = query.Expression{}
				continue
			} else if strings.ToUpper(token) == "," {
				p.step = stepSelectExpression
				p.query.SelectExpressionsList = append(p.query.SelectExpressionsList, currentExpresion)
				currentExpresion = query.Expression{}
				continue
			} else {
				query.AddValueToExpression(&currentExpresion, token)
			}
			p.step = stepSelectExpression
		case stepSelectMainTable:
			table := p.peekNextToken()

			if len(table) == 0 {
				return p.query, fmt.Errorf("expected table name")
			}
			p.query.Table = table
			p.step = stepEnd
		case stepAfterTable:
			token := p.peekNextToken()
			switch token {
			case ";":
				p.step = stepWhereExpression
			default:
				p.step = stepEnd
			}
		case stepWhere:
			fmt.Printf("WHERE")
			currentExpresion = query.Expression{}
			p.step = stepWhereExpression
		case stepWhereExpression:
			fmt.Printf("WHERE EXPRESSION")
			token := p.peekNextToken()
			query.AddValueToExpression(&currentExpresion, token)
			p.step = stepEnd
		case stepOrderby:
			fmt.Printf("Order by")
		case stepOrderbyExpression:
			token := p.peekNextToken()
			fmt.Printf(token)
		case stepEnd:
			//TODO update this part
			return p.query, nil
		}
	}
}

func (p *parser) handleType() bool {
	token := p.peekNextToken()
	switch token {
	case "select":
		p.query.QueryType = query.Select
		p.step = stepSelectExpression
		return false
	default:
		return true
	}
}

func (p *parser) handleWhereExpression() {
	return
}

// Parses and handles select
func (p *parser) handleSelectExpression() {
	return
}

// Returns next token
func (p *parser) peekNextToken() string {
	return p.tokenizer.nextToken()
}
