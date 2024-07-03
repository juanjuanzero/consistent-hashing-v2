package hashring_test

import (
	"fmt"
	"testing"

	hashring "github.com/juanjuanzero/consistent-hashing-v2/internal"
)

func TestHashRing(t *testing.T) {
	// setup
	t.Run("can ingest and retrieve reliably", func(t *testing.T) {
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
	})
	t.Run("can handle updates to the vector clock", func(t *testing.T) {
		hashRing := hashring.NewHashRing(3)
		// add data to hashring
		dataMap := make(map[string]string)
		for i := range 10 {
			dataValue := fmt.Sprintf("Node Data:%v", i)
			key := fmt.Sprintf("key:%v", i)
			hashRing.AddData(key, dataValue)
			dataMap[key] = dataValue
		}

		for i, v := range dataMap {
			// get the value
			value, err := hashRing.GetData(i)
			if err != nil {
				t.Errorf("error getting values from hashring %v", err)
			}
			if v != value.Value {
				t.Error("invalid values...")
			}
			// update it
			newValue := value.Value + fmt.Sprintf("%v", i)
			err = hashRing.AddData(i, newValue)
			if err != nil {
				t.Error("error adding data to hashring")
			}
			dataMap[i] = newValue
		}

		// look for the values in the data map
		for k, v := range dataMap {
			retrieved, err := hashRing.GetData(k)
			if err != nil {
				t.Errorf("error in test getting expected %v, error: %v ", v, err)
			}
			if retrieved.Value != v {
				t.Errorf("got wrong value %v, expected %v", retrieved, v)
			}
		}
	})

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
			{VectorClock: []int{0, 1, 0, 0, 0}, Value: "1"},
			{VectorClock: []int{0, 2, 0, 0, 0}, Value: "2"},
			{VectorClock: []int{0, 0, 0, 0, 0}, Value: "3"},
		}
		result := hashRing.ResolveToUpdated(toResolve)
		if result.Value != "2" {
			t.Errorf("got wrong value %v, expected %v", result.Value, "2")
		}
	})

}
