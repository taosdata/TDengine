package app

import (
	"regexp"
	"strings"
)

type RouteMatchCriteria struct {
	Tag   string         `yaml:"tag"`
	Value string         `yaml:"match"`
	Re    *regexp.Regexp `yaml:"-"`
}

func (c *RouteMatchCriteria) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var v map[string]string
	if e := unmarshal(&v); e != nil {
		return e
	}

	for k, a := range v {
		c.Tag = k
		c.Value = a
		if strings.HasPrefix(a, "re:") {
			re, e := regexp.Compile(a[3:])
			if e != nil {
				return e
			}
			c.Re = re
		}
	}

	return nil
}

type Route struct {
	Continue       bool                 `yaml:"continue"`
	Receiver       string               `yaml:"receiver"`
	GroupWait      Duration             `yaml:"group_wait"`
	GroupInterval  Duration             `yaml:"group_interval"`
	RepeatInterval Duration             `yaml:"repeat_interval"`
	GroupBy        []string             `yaml:"group_by"`
	Match          []RouteMatchCriteria `yaml:"match"`
	Routes         []*Route             `yaml:"routes"`
}
