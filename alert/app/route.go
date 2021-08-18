/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
