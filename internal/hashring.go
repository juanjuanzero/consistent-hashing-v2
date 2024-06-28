package hashring

import (
	"cmp"
	"crypto/sha1"
	"fmt"
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
func (hr *HashRing) AddData(key, value string) error {
	// find the node responsible for the data
	hashed := hashString(key)
	nodes, err := hr.GetNodes(hashed, value)
	if err != nil {
		return fmt.Errorf("error retrieving all of the nodes %v", err)
	}
	chosen := hr.PickOne(nodes)
	// add it to that node
	chosen.AddData(hashed, value)

	return nil
}

// add a function that will get all of the data from the primary and the replicas
func (hr *HashRing) GetNodes(hashedKey, value string) ([]*Node, error) {
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
	index := rand.Intn(len(nodes) - 1)
	return nodes[index]
}

// add a function that will pick the most recent update from all of the data using vector clocks

// a method to get data
func (hr *HashRing) GetData(key string) (string, error) {
	// hash the key of the incoming data
	hashed := hashString(key)

	// find the node where this is stored
	primaryNode, err := hr.GetNode(hashed)
	if err != nil {
		return "", fmt.Errorf("failed to get data using %v, err: %v", key, err)
	}
	if primaryNode == nil {
		return "", fmt.Errorf("no node found for key %v, err: %v", key, err)
	}
	// get it from that node
	// TODO eventually do quorum read
	return primaryNode.Data[hashed], nil
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
	Data        map[string]string
	VectorClock []int
}

func (n *Node) AddData(key, value string) {
	n.Data[key] = value
}
func (n *Node) GetData(key string) (string, error) {
	value, ok := n.Data[key]
	if !ok {
		return "", fmt.Errorf("value for key %v not found", key)
	}
	return value, nil
}

func NewNode(hash, name string) *Node {
	data := make(map[string]string)
	return &Node{
		HashValue: hash,
		Name:      name,
		Data:      data,
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
		node := NewNode(string(hashName), name)
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
