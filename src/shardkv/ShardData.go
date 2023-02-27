package shardkv

type ShardData struct {
	State    int
	Data     map[string]string
	LastResp map[int64]LastRespData
}

func NewShardData() *ShardData {
	t := ShardData{
		State:    Server,
		Data:     make(map[string]string),
		LastResp: make(map[int64]LastRespData),
	}
	return &t
}
func (sd *ShardData) Get(key string) string {
	return sd.Data[key]
}

func (sd *ShardData) Put(key string, value string) {
	sd.Data[key] = value
}

func (sd *ShardData) Append(key string, value string) {
	sd.Data[key] += value
}
func (sd *ShardData) CompareIndex(client, req int64) bool {
	if v, ok := sd.LastResp[client]; !ok || v.RequestId < req {
		return true
	}
	return false
}
func (sd *ShardData) DeepCopy() ShardData {
	t := make([]ShardData, 1)
	src := []ShardData{*sd}
	copy(t, src)
	return t[0]
}
