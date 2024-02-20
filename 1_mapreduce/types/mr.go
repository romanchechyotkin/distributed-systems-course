package types

type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type KeyValue struct {
	Key   string
	Value string
}
