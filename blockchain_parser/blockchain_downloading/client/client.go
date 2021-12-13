package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// https://www.blockchain.com/api/blockchain_api

const (
	getRawTxURL       = "https://blockchain.info/rawtx/%s?format=hex"
	getRawBlockURL    = "https://blockchain.info/rawblock/%s?format=hex"
	getBlockHeightURL = "https://blockchain.info/block-height/%d?format=json"
)

type Client interface {
	GetRawTx(hash string) (string, error)
	GetRawBlockByHash(hash string) (string, error)
	GetRawBlockByHeight(h uint32) (string, error)
	GetBlockJSONByHeight(h uint32) ([]byte, error)
}

type SimpleClient struct {
	debug bool
}

type Bitcoin_Block struct {
	Hash        string
	Ver         int
	Prev_block  string
	Mrkl_root   string
	Time        int
	Bits        int
	Next_block  []string
	Fee         int
	Nonce       int
	N_tx        int
	Size        int
	Block_index int
	Main_chain  bool
	Height      int
	Weight      int
	Tx          []Transaction
}

type Transaction struct {
	Hash         string
	Ver          int
	Vin_sz       int
	Vout_sz      int
	Size         int
	Weight       int
	Fee          int
	Relayed_by   string
	Lock_time    int
	Tx_index     int64
	Double_spend bool
	Time         int
	Block_index  int
	Block_height int
	Inputs       []Input
	Out          []Output
}

type Input struct {
	Sequence int
	Witness  string
	Script   string
	Index    int
	Prev_out Output
}

type Output struct {
	Output_type int `json:"type"`
	Spent       bool
	Value       int
	//Spending_outpoints []int
	N        int
	Tx_index int64
	Script   string
	Addr     string
}

type Bitcoin_Block_Json struct {
	Blocks []Bitcoin_Block `json:"blocks"`
}

func NewSimpleClient() Client {
	return SimpleClient{}
}

func (c SimpleClient) GetRawTx(hash string) (string, error) {
	url := fmt.Sprintf(getRawTxURL, hash)
	if c.debug {
		fmt.Println("GET", url)
	}
	resp, err := get(url)
	return string(resp), err
}

func (c SimpleClient) GetRawBlockByHash(hash string) (string, error) {
	url := fmt.Sprintf(getRawBlockURL, hash)
	if c.debug {
		fmt.Println("GET", url)
	}
	resp, err := get(url)
	return string(resp), err
}

func (c SimpleClient) GetRawBlockByHeight(h uint32) (string, error) {
	hash, err := c.getBlockHashByHeight(h)
	if err != nil {
		return "", err
	}

	return c.GetRawBlockByHash(hash)
}

func (c SimpleClient) getBlockHashByHeight(h uint32) (string, error) {

	type Block struct {
		Hash string `json:"hash"`
	}
	type Result struct {
		Blocks []Block `json:"blocks"`
	}

	url := fmt.Sprintf(getBlockHeightURL, h)
	if c.debug {
		fmt.Println("GET", url)
	}
	resp, err := get(url)
	if err != nil {
		return "", err
	}

	result := Result{}
	err = json.Unmarshal(resp, &result)
	if err != nil {
		if c.debug {
			fmt.Println("resp:", string(resp))
		}
		return "", err
	}

	if n := len(result.Blocks); n != 1 {
		return "", fmt.Errorf("blocks: %d", n)
	}
	return result.Blocks[0].Hash, nil
}

func (c SimpleClient) GetBlockJSONByHeight(h uint32) ([]byte, error) {

	url := fmt.Sprintf(getBlockHeightURL, h)
	if c.debug {
		fmt.Println("GET", url)
	}
	resp, err := get(url)
	if err != nil {
		return nil, err
	}

	b_b_j := Bitcoin_Block_Json{}
	err = json.Unmarshal(resp, &b_b_j)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return json.MarshalIndent(b_b_j, "", "\t")
}

func get(url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}
