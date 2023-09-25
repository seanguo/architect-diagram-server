package node

type Manager struct {
	nodes map[string]Node
}

func NewManager() *Manager {
	return &Manager{
		nodes: make(map[string]Node),
	}
}

func (m *Manager) Add(node Node) {
	m.nodes[node.GetID()] = node
}

func (m *Manager) Remove(node Node) {
	delete(m.nodes, node.GetID())
}

func (m *Manager) Get(nodeId string) Node {
	return m.nodes[nodeId]
}
