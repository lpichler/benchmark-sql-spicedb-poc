package client

import (
	"fmt"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func InitServer(spiceDBURL string, preSharedKey string) *authzed.Client {
	SpiceDbClient, err := GetSpiceDbClient(spiceDBURL, preSharedKey)
	if err != nil {
		err := fmt.Errorf("%v", err)
		fmt.Println(err)
		os.Exit(1)
	}
	return SpiceDbClient
}

func GetSpiceDbClient(endpoint string, presharedKey string) (*authzed.Client, error) {
	fmt.Println("Attempting to connect to spicedb...")
	defer func() {
		fmt.Println("Connection to spicedb established")
	}()

	var opts []grpc.DialOption

	opts = append(opts, grpc.WithBlock())

	opts = append(opts, grpcutil.WithInsecureBearerToken(presharedKey))
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	return authzed.NewClient(
		endpoint,
		opts...,
	)
}
