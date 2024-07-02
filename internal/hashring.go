package hashring

import (
	"cmp"
	"crypto/sha1"
	"fmt"
	"log"
	"math/rand"
	"slices"
)

type HashRing struct {
	SortedNodeHashes []string            // a list of nodes to be hashed
	Nodes            map[string]*Node    // a map of nodes to access things
	NodeToReplica    map[string][]string // map of node to replica hashes
	ReplicaCount     int
	NodeCount        int
}

// a method to add data
func (hr *HashRing) AddData(key string, value KeyValueData) error {
	// find the node responsible for the data
	hashed := hashString(key)
	value.HashedKey = hashed
	nodes, err := hr.GetNodes(hashed)
	if err != nil {
		return fmt.Errorf("error retrieving all of the nodes %v", err)
	}
	chosen := hr.PickOne(nodes)
	// add it to that node
	chosen.AddData(hashed, value)

	return nil
}

// add a function that will get all of the data from the primary and the replicas
func (hr *HashRing) GetNodes(hashedKey string) ([]*Node, error) {
	// find the node where this is stored
	var nodes []*Node
	primaryNode, err := hr.GetNode(hashedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get data using %v", err)
	}
	if primaryNode == nil {
		return nil, fmt.Errorf("failed to get data using %v", err)
	}
	nodes = append(nodes, primaryNode)
	// get the replicas
	for _, replicaHash := range hr.NodeToReplica[primaryNode.HashValue] {
		replica, err := hr.GetNode(replicaHash)
		if err != nil {
			return nil, fmt.Errorf("failed to replica data using %v", err)
		}
		if replica == nil {
			return nil, fmt.Errorf("failed to get data using %v", err)
		}
		nodes = append(nodes, replica)
	}
	return nodes, nil

}

// add a function that will randomly pick which node to save the data to
func (hr *HashRing) PickOne(nodes []*Node) *Node {
	index := rand.Intn(len(nodes))
	return nodes[index]
}

// add a function that will pick the most recent update from all of the data using vector clocks
func (hr *HashRing) ResolveToUpdated(toResolve []KeyValueData) KeyValueData {
	// look at the vector clocks of each and find the one that has
	// an update is valid if the vector clock of the node is valid at the
	if len(toResolve) == 1 {
		return toResolve[0]
	}
	var mostUpdate KeyValueData
	for i, iNode := range toResolve {
		for j, jNode := range toResolve {
			if i == j {
				// its the same element
				continue
			} else {
				// compare vector clocks
				// find the one with the most up to date information i.e. the latest vector clock
				// a vector clock is happened before another if all of the elements are <= the other's elements
				result := CmpElementWise(iNode.VectorClock, jNode.VectorClock)
				if result == 1 {
					// compare it to the mostUptoDate
					isLater := CmpElementWise(iNode.VectorClock, mostUpdate.VectorClock)
					if isLater == 1 {
						mostUpdate = iNode
					}
				}
			}
		}
	}
	return mostUpdate
}

// CmpElementWise takes in two slices of ints and compare them element wise
// -1 if all elements in a are less than b
// 1 if all elements in a are greater than b
// 0 if not all elements are less than or equal
// the skip is index that should be skipped
func CmpElementWise(a, b []int) int {
	truthTable := make([]int, len(a))
	for i := range len(a) {
		if a[i] == b[i] {
			truthTable[i] = 0
			continue
		}
		if a[i] < b[i] {
			truthTable[i] = -1
			continue
		}
		if a[i] > b[i] {
			truthTable[i] = 1
			continue
		}
	}

	position := 0
	for _, val := range truthTable {
		if val == 0 || val == position {
			// if its the same as before go next
			continue
		}
		if val == -1*position {
			// if there is a change in direction, then not all elements fit
			return 0
		}
		// val is position
		position = val
	}

	return position
}

// a method to get data
func (hr *HashRing) GetData(key string) (KeyValueData, error) {
	// hash the key of the incoming data
	hashed := hashString(key)

	// find the node where this is stored
	nodes, err := hr.GetNodes(hashed)
	if err != nil {
		return KeyValueData{}, fmt.Errorf("failed to get data using %v, err: %v", key, err)
	}
	if nodes == nil {
		return KeyValueData{}, fmt.Errorf("no node found for key %v, err: %v", key, err)
	}
	// get it from that node
	toResolve := []KeyValueData{}
	for _, node := range nodes {
		data, err := node.GetData(hashed)
		if err != nil {
			log.Printf("key %v not found", key)
			continue
		}
		toResolve = append(toResolve, data)

	}
	data := hr.ResolveToUpdated(toResolve)
	return data, nil
}

// finds the node that would be responsible for this hash
// in consistent hashing it would be data up to and including the data
func (hr *HashRing) GetNode(hash string) (*Node, error) {
	// loop across all of the node names and compare
	for i, nodeHash := range hr.SortedNodeHashes {
		if cmp.Compare(hash, nodeHash) == -1 || cmp.Compare(hash, nodeHash) == 0 {
			// it is less than this node
			return hr.Nodes[nodeHash], nil
		}
		if cmp.Compare(hash, nodeHash) == 1 && i == hr.NodeCount-1 {
			// if we are at the end then it is the first one...
			return hr.Nodes[hr.SortedNodeHashes[0]], nil
		}
	}
	return nil, fmt.Errorf("unable to find node with hash %v", hash)
}

func hashString(key string) string {
	hasher := sha1.New()
	return string(hasher.Sum([]byte(key)))
}

type Node struct {
	Name        string
	HashValue   string
	Data        map[string]KeyValueData
	VectorClock []int
	VectorIndex int
}

type KeyValueData struct {
	Key         string
	HashedKey   string
	Value       string
	VectorClock []int
}

func (n *Node) AddData(key string, value KeyValueData) {
	// increment the vector clock of the node
	n.VectorClock[n.VectorIndex]++
	// increment the vector clock of the message for metadata
	value.VectorClock[n.VectorIndex]++
	n.Data[key] = value
}
func (n *Node) GetData(key string) (KeyValueData, error) {
	value, ok := n.Data[key]
	if !ok {
		return KeyValueData{}, fmt.Errorf("value for key %v not found", key)
	}
	return value, nil
}

func NewNode(hash, name string, vectorIndex int, nodeCount int) *Node {
	data := make(map[string]KeyValueData)
	vectorClock := make([]int, nodeCount)
	return &Node{
		HashValue:   hash,
		Name:        name,
		Data:        data,
		VectorIndex: vectorIndex,
		VectorClock: vectorClock,
	}
}

// give data a shape to also contain the vector clock as metadata

func NewHashRing(nodeCount int) *HashRing {
	// for at least the number of replicas create nodes in the hash ring
	// what to hash with?
	hasher := sha1.New()
	// create at least 3 nodes
	var nodes []*Node
	var nodeHashes []string
	if nodeCount < 3 {
		nodeCount = 3
	}
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("Node%v", i)
		hashName := hasher.Sum([]byte(name))
		node := NewNode(string(hashName), name, i, nodeCount)
		nodes = append(nodes, node)
	}

	slices.SortFunc(nodes, func(a *Node, b *Node) int {
		return cmp.Compare(a.HashValue, b.HashValue)
	})

	// create a set of nodes with corresponding replicas
	nodeMap := make(map[string]*Node)
	nodeToReplicaMap := make(map[string][]string)

	for i, v := range nodes {
		nodeMap[v.HashValue] = v
		next := GetNextElement(i, len(nodes), 2)
		nextReplica := GetNextElement(next, len(nodes), 2)
		nodeToReplicaMap[v.HashValue] = []string{nodes[next].HashValue, nodes[nextReplica].HashValue}
		nodeHashes = append(nodeHashes, v.HashValue)
	}

	return &HashRing{
		SortedNodeHashes: nodeHashes,
		Nodes:            nodeMap,
		NodeToReplica:    nodeToReplicaMap,
		ReplicaCount:     2,
		NodeCount:        nodeCount,
	}
}

func GetNextElement(index, length, replicaCount int) int {
	if index == length-1 {
		return 0
	}
	return index + 1
}
