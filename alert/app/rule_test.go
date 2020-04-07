package app

import (
	"fmt"
	"testing"

	"github.com/taosdata/alert/utils/log"
)

func TestParseGroupBy(t *testing.T) {
	cases := []struct {
		sql  string
		cols []string
	}{
		{
			sql:  "select * from a",
			cols: []string{},
		},
		{
			sql:  "select * from a group by abc",
			cols: []string{"abc"},
		},
		{
			sql:  "select * from a group by abc, def",
			cols: []string{"abc", "def"},
		},
		{
			sql:  "select * from a Group by abc, def order by abc",
			cols: []string{"abc", "def"},
		},
	}

	for _, c := range cases {
		cols, e := parseGroupBy(c.sql)
		if e != nil {
			t.Errorf("failed to parse sql '%s': %s", c.sql, e.Error())
		}
		for i := range cols {
			if i >= len(c.cols) {
				t.Errorf("count of group by columns of '%s' is wrong", c.sql)
			}
			if c.cols[i] != cols[i] {
				t.Errorf("wrong group by columns for '%s'", c.sql)
			}
		}
	}
}

func TestManagement(t *testing.T) {
	const format = `{"name":"rule%d", "sql":"select count(*) as count from meters", "expr":"count>2"}`

	log.Init()

	for i := 0; i < 5; i++ {
		s := fmt.Sprintf(format, i)
		rule, e := newRule(s)
		if e != nil {
			t.Errorf("failed to create rule: %s", e.Error())
		}
		e = doUpdateRule(rule, s)
		if e != nil {
			t.Errorf("failed to add or update rule: %s", e.Error())
		}
	}

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("rule%d", i)
		if _, ok := rules.Load(name); !ok {
			t.Errorf("rule '%s' does not exist", name)
		}
	}

	name := "rule1"
	if e := doDeleteRule(name); e != nil {
		t.Errorf("failed to delete rule: %s", e.Error())
	}

	if _, ok := rules.Load(name); ok {
		t.Errorf("rule '%s' should not exist any more", name)
	}
}
