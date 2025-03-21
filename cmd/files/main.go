package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	assetpb "cloud.google.com/go/asset/apiv1/assetpb"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Specify the directory containing the protobuf files.
	dirPath := "/Users/kostam/tmp/proto" // change this to your directory

	// Open the directory.
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		log.Fatalf("Failed to read directory %s: %v", dirPath, err)
	}

	types := map[string]struct{}{}

	// Iterate over the files in the directory.
	for _, file := range files {
		// Skip directories.
		if file.IsDir() {
			continue
		}

		// Build the full path for the file.
		filePath := filepath.Join(dirPath, file.Name())

		// Read the file contents.
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read file %s: %v\n", filePath, err)
			continue
		}

		// Unmarshal the protobuf data into an Asset message.
		var asset assetpb.Asset
		if err := proto.Unmarshal(data, &asset); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to unmarshal file %s: %v\n", filePath, err)
			continue
		}

		types[asset.AssetType] = struct{}{}
	}

	for k := range types {
		fmt.Printf("%s\n", k)
	}
}
