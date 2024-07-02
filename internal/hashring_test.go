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
		dataValue := fmt.Sprintf("Node Data:%v", i)
		key := fmt.Sprintf("key:%v", i)
		hashRing.AddData(key, dataValue)
		dataMap[key] = dataValue
	}

	for k, v := range dataMap {
		retrieved, err := hashRing.GetData(k)
		if err != nil {
			t.Errorf("error in test getting expected %v, error: %v ", v, err)
		}
		if retrieved.Value != v {
			t.Errorf("got wrong value %v, expected %v", retrieved, v)
		}
	}

}

func TestCmpElementWise(t *testing.T) {
	// setup
	t.Run("vectors are the same", func(t *testing.T) {
		a := []int{1, 0, 0, 0}
		b := []int{1, 0, 0, 0}
		result := hashring.CmpElementWise(a, b)
		if result != 0 {
			t.Errorf("got wrong value %v, expected %v", result, 0)
		}
	})

	t.Run("a happens before b", func(t *testing.T) {
		a := []int{0, 0, 0, 0}
		b := []int{1, 1, 0, 0}
		result := hashring.CmpElementWise(a, b)
		expect := -1
		if result != expect {
			t.Errorf("got wrong value %v, expected %v", result, expect)
		}
	})
	t.Run("a happens after b", func(t *testing.T) {
		a := []int{1, 1, 0, 0}
		b := []int{0, 0, 0, 0}
		result := hashring.CmpElementWise(a, b)
		expect := 1
		if result != expect {
			t.Errorf("got wrong value %v, expected %v", result, expect)
		}
	})
	t.Run("a and b not ordered", func(t *testing.T) {
		a := []int{1, 1, 0, 0}
		b := []int{0, 0, 1, 1}
		result := hashring.CmpElementWise(a, b)
		expect := 0
		if result != expect {
			t.Errorf("got wrong value %v, expected %v", result, expect)
		}
	})
}

func TestResolveUpdated(t *testing.T) {
	hashRing := hashring.NewHashRing(5)
	t.Run("resolves to the latest update on all of the nodes", func(t *testing.T) {
		toResolve := []hashring.KeyValueData{
			{VectorClock: []int{0, 0, 0, 0, 0}, Value: "0"},
			{VectorClock: []int{0, 0, 0, 0, 0}, Value: "1"},
			{VectorClock: []int{0, 1, 0, 0, 0}, Value: "2"},
			{VectorClock: []int{0, 0, 0, 0, 0}, Value: "3"},
		}
		result := hashRing.ResolveToUpdated(toResolve)
		if result.Value != "2" {
			t.Errorf("got wrong value %v, expected %v", result.Value, "2")
		}
	})

}
