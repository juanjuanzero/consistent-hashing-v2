package hashring

import (
	"cmp"
	"crypto/sha1"
	"fmt"
	"slices"
)

type HashRing struct {
	SortedNodeHashes []string            // a list of nodes to be hashed
	Nodes            map[string]Node     // a map of nodes to access things
	NodeToReplica    map[string][]string // map of node to replica hashes
	ReplicaCount     int
	NodeCount        int
}

// a method to add data
func (hr *HashRing) AddData(key, value string) error {
	// hash the key of the incoming data
	hashed := hashString(key)

	// find the node where this is stored
	primaryNode, err := hr.GetNode(hashed)
	if err != nil {
		return fmt.Errorf("failed to get data using %v", err)
	}
	// find the node responsible for the data
	// add it to that node
	primaryNode.Data[hashed] = value
	return nil
}

// a method to get data
func (hr *HashRing) GetData(key string) (string, error) {
	// hash the key of the incoming data
	hashed := hashString(key)

	// find the node where this is stored
	primaryNode, err := hr.GetNode(hashed)
	if err != nil {
		return "", fmt.Errorf("failed to get data using %v", err)
	}
	// get it from that node
	// TODO eventually do quorum read
	return primaryNode.Data[hashed], nil
}

// finds the node that would be responsible for this hash
// in consistent hashing it would be data up to and including the data
func (hr *HashRing) GetNode(hash string) (Node, error) {
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
	return Node{}, fmt.Errorf("unable to find node with hash %v", hash)
}

func hashString(key string) string {
	hasher := sha1.New()
	return string(hasher.Sum([]byte(key)))
}

type Node struct {
	Name      string
	HashValue string
	Data      map[string]string
}

func NewNode(hash, name string) Node {
	data := make(map[string]string)
	return Node{
		HashValue: hash,
		Name:      name,
		Data:      data,
	}
}

func NewHashRing(nodeCount int) HashRing {
	// for at least the number of replicas create nodes in the hash ring
	// what to hash with?
	hasher := sha1.New()
	// create at least 3 nodes
	var nodes []Node
	var nodeHashes []string
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("Node::%v", i)
		hashName := hasher.Sum([]byte(name))
		node := NewNode(string(hashName), name)
		nodes = append(nodes, node)
	}

	slices.SortFunc(nodes, func(a Node, b Node) int {
		return cmp.Compare(a.HashValue, b.HashValue)
	})

	// create a set of nodes with corresponding replicas
	nodeMap := make(map[string]Node)
	nodeToReplicaMap := make(map[string][]string)

	for i, v := range nodes {
		nodeMap[v.Name] = v
		next := GetNextElement(i, len(nodes))
		nodeToReplicaMap[v.HashValue] = []string{nodes[next].HashValue, nodes[next+1].HashValue}
		nodeHashes = append(nodeHashes, string(v.HashValue))
	}

	return HashRing{
		SortedNodeHashes: nodeHashes,
		Nodes:            nodeMap,
		NodeToReplica:    nodeToReplicaMap,
		ReplicaCount:     2,
		NodeCount:        nodeCount,
	}
}

func GetNextElement(index, length int) int {
	if index == length-1 {
		return 0
	}
	return index + 1
}
