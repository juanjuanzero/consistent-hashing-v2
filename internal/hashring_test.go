package hashring_test

import (
	"fmt"
	"testing"

	hashring "github.com/juanjuanzero/consistent-hashing-v2/internal"
)

func TestHashRing(t *testing.T) {
	// setup
	// create a hashring
	hashRing := hashring.NewHashRing(3)
	// add data to hashring
	dataMap := make(map[string]string)
	for i := range 10 {
		data := fmt.Sprintf("Node Data:%v", i)
		key := fmt.Sprintf("key:%v", i)
		hashRing.AddData(key, data)
		dataMap[key] = data
	}
	// can be deleted
	for nodehash, node := range hashRing.Nodes {
		for key, value := range node.Data {
			t.Logf("nodeHash:%v, key: %v, value: %v", nodehash, key, value)
		}
	}
	// test
	for k, v := range dataMap {
		retrieved, err := hashRing.GetData(k)
		if err != nil {
			t.Errorf("error in test getting expected %v, error: %v ", v, err)
		}
		if retrieved != v {
			t.Errorf("got wrong value %v, expected %v", retrieved, v)
		}
	}

}
