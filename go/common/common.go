package common

type Options struct {
	Num int `long:"num"  short:"n" description:"Number of workers"`

	File string `short:"f" long:"file" description:"A file" value-name:"FILE"`

	Type string `long:"type" description:"Bin type"`

	Port string `long:"port" description:"Running port"`

	Id string `long:"id" description:"Id of worker"`
}

type StoreRequest struct {
	ObjectId  string `json:"object_id"`
	RequestId string `json:"request_id"`
	Hash      string `json:"hash"`
	Size      int    `json:"size"`
	Action    string `json:"action"`
}

type StoreResponse struct {
	Result []string `json:"result"`
}
