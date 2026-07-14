package kvStore

type Store interface {
	Set(key, value string)
	Get(key string) (string, bool)
}