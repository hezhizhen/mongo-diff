package main

import (
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	session := connect("127.0.0.1:27017")
	defer session.Close()

	oldDB, newDB := session.DB("old"), session.DB("new")

	oldCols, err := oldDB.CollectionNames()
	must(err)
	newCols, err := newDB.CollectionNames()
	must(err)
	if len(oldCols) != len(newCols) {
		panic("mismatched collection number")
	}

	for _, col := range oldCols {
		oldC := oldDB.C(col)
		newC := newDB.C(col)
		compareCollections(oldC, newC)
	}
}

func connect(host string) *mgo.Session {
	s, err := mgo.DialWithTimeout(host, time.Second*10)
	must(err)
	s.SetSocketTimeout(time.Second * 5)
	s.SetSyncTimeout(time.Second * 5)
	return s
}

func compareCollections(oldC, newC *mgo.Collection) {
	if oldC.Name != newC.Name {
		panic(fmt.Sprintf(
			"irrelevant collections: old: %s, new: %s",
			oldC.Name, newC.Name,
		))
	}
	var oldAll, newAll []bson.M
	must(oldC.Find(bson.D{}).All(&oldAll))
	must(newC.Find(bson.D{}).All(&newAll))

	added := computeDiff(newAll, oldAll)
	removed := computeDiff(oldAll, newAll)

	printDiff(newC.Name, added, removed)
}

// computeDiff computes a-b
func computeDiff(a, b []bson.M) []bson.M {
	m := make(map[string]bson.M, len(b))
	for _, one := range b {
		id := one["_id"].(bson.ObjectId).String()
		m[id] = one
	}
	var ret []bson.M
	for _, one := range a {
		id := one["_id"].(bson.ObjectId).String()
		if _, ok := m[id]; ok {
			// TODO: compare data?
			continue
		}
		ret = append(ret, one)
	}
	return ret
}

func printDiff(col string, added, removed []bson.M) {
	fmt.Println(col)
	fmt.Printf("%s added: %d\n", col, len(added))
	for _, one := range added {
		fmt.Println(one["_id"])
	}
	fmt.Printf("%s removed: %d\n", col, len(removed))
	for _, one := range removed {
		fmt.Println(one["_id"])
	}
}
