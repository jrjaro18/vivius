package node

type DataStore interface {
	Set(key, value string)
	Get(key string) (string, bool)
	Delete(key string)
}