package dynamodbfriend

import (
	"fmt"
	"strings"
)

type nameSet struct {
	names map[string]struct{}
}

func newNameSet(names ...string) *nameSet {
	ns := &nameSet{
		names: map[string]struct{}{},
	}
	for _, name := range names {
		ns.names[name] = struct{}{}
	}
	return ns
}

func (ns *nameSet) Insert(names ...string) {
	for _, name := range names {
		ns.names[name] = struct{}{}
	}
}

func (ns *nameSet) Remove(names ...string) {
	for _, name := range names {
		delete(ns.names, name)
	}
}

func (ns *nameSet) Contains(name string) bool {
	_, found := ns.names[name]
	return found
}

func (ns *nameSet) Names() []string {
	names := []string{}
	for name := range ns.names {
		names = append(names, name)
	}
	return names
}

func (ns *nameSet) Empty() bool {
	return len(ns.names) == 0
}

func (ns *nameSet) String() string {
	names := ns.Names()
	if len(names) == 0 {
		return "[]"
	}
	return fmt.Sprintf("[\"%s\"]", strings.Join(names, "\", \""))
}
