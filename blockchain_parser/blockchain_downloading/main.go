package main

import (
	"bitcoin_blockchain_downloading/client"
	"fmt"
	"os"
)

func deal_with_error(err error) {
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func stringToBin(s string) (binString string) {
	for _, c := range s {
		binString = fmt.Sprintf("%s%b", binString, c)
	}
	return
}

func download_rawdata(start uint32, end uint32) {
	client := client.NewSimpleClient()
	for i := start; i <= end; i++ {
		rawBlock, err := client.GetRawBlockByHeight(i)
		deal_with_error((err))

		cur_block_name := fmt.Sprintf("blockchain/RAW/blk%d.dat", i)
		cur_block_file, err2 := os.Create(cur_block_name)
		deal_with_error(err2)
		// res := []byte(strings.ToUpper(rawBlock))
		// res2 := hex.EncodeToString(res)
		_, err3 := cur_block_file.WriteString(rawBlock)
		deal_with_error(err3)

		fmt.Printf("downloaded block %d\n", i)
	}
}

func download_JSON(start uint32, end uint32) {
	client := client.NewSimpleClient()
	for i := start; i <= end; i++ {
		block_json, err := client.GetBlockJSONByHeight(i)
		deal_with_error((err))

		cur_block_name := fmt.Sprintf("blockchain/JSON/blk%d.json", i)
		cur_block_file, err2 := os.Create(cur_block_name)
		deal_with_error(err2)
		_, err3 := cur_block_file.Write(block_json)
		deal_with_error(err3)

		//fmt.Printf("block#%d: %s\n", i, block_json)
	}
}

func main() {
	download_rawdata(710911, 710911)
	// download_JSON(710793, 710793)
}
