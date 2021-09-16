package btree_test

import (
	"fmt"
	"os"
	"testing"
)

func TestBtree(t *testing.T) {
	wd, _ := os.Getwd()
	fmt.Println("btree test1")
	t.Log("test is here", wd)
}

func TestBtree2(t *testing.T) {
	wd, _ := os.Getwd()
	t.Log("test is here", wd)
}
