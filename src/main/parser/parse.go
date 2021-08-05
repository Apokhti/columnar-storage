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
			switch strings.ToUpper(p.peekNextToken()) {
			case "SELECT":
				p.query.QueryType = query.Select
				p.step = stepSelectExpression
			default:
				return p.query, fmt.Errorf("invalid query type")
			}
		case stepSelectExpression:

			token := p.peekNextToken()

			if strings.ToUpper(token) == "FROM" {
				p.step = stepSelectMainTable
				p.query.ExpressionsList = append(p.query.ExpressionsList, currentExpresion)
				currentExpresion = query.Expression{}
				continue
			} else if strings.ToUpper(token) == "," {
				p.step = stepSelectExpression
				p.query.ExpressionsList = append(p.query.ExpressionsList, currentExpresion)
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
