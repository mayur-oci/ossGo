package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/oracle/oci-go-sdk/v36/common"
	"github.com/oracle/oci-go-sdk/v36/example/helpers"
	"github.com/oracle/oci-go-sdk/v36/streaming"
)

func main() {
	fmt.Println("go running")

	streamAdminClient, err := streaming.NewStreamAdminClientWithConfigurationProvider(common.DefaultConfigProvider())
	helpers.FatalIfError(err)
	cmptID := "ocid1.tenancy.oc1..aaaaaaaaopbu45aomik7sswe4nzzll3f6ii6pipd5ttw4ayoozez37qqmh3a"

	// Create a request and dependent object(s).

	req := streaming.CreateStreamPoolRequest{
		CreateStreamPoolDetails: streaming.CreateStreamPoolDetails{Name: common.String("streampool-go-example"),
			CompartmentId: common.String(cmptID)},
		OpcRequestId:    new(string),
		OpcRetryToken:   new(string),
		RequestMetadata: common.RequestMetadata{},
	}

	// Send the request using the service client
	streampoolRsp, err := streamAdminClient.CreateStreamPool(context.Background(), req)

	helpers.FatalIfError(err)

	// Retrieve value from the response.
	fmt.Println(streampoolRsp)

	//TODO wait for streampool creation with OCI SDK
	time.Sleep(15 * time.Second)

	streamReq := streaming.CreateStreamRequest{
		CreateStreamDetails: streaming.CreateStreamDetails{Name: common.String("stream-go-example"),
			Partitions:       common.Int(2),
			RetentionInHours: common.Int(24),
			StreamPoolId:     common.String(*streampoolRsp.Id)}}

	// Send the request using the service client
	streamRsp, err := streamAdminClient.CreateStream(context.Background(), streamReq)

	helpers.FatalIfError(err)

	// Retrieve value from the response.
	fmt.Println(streamRsp)

	//TODO wait for stream creation with OCI SDK
	time.Sleep(15 * time.Second)

	streamClient, err := streaming.NewStreamClientWithConfigurationProvider(common.DefaultConfigProvider(), *streamRsp.MessagesEndpoint)
	helpers.FatalIfError(err)

	// Create a request and dependent object(s).
	for i := 0; i < 5; i++ {
		putMsgReq := streaming.PutMessagesRequest{StreamId: common.String(*streamRsp.Id),
			PutMessagesDetails: streaming.PutMessagesDetails{
				Messages: []streaming.PutMessagesDetailsEntry{
					{Key: []byte("key dummy" + strconv.Itoa(i)),
						Value: []byte("value dummy" + strconv.Itoa(i))},
					{Key: []byte("key dummy-" + strconv.Itoa(i)),
						Value: []byte("value dummy-" + strconv.Itoa(i))}}},
		}

		// Send the request using the service client
		putMsgResp, err := streamClient.PutMessages(context.Background(), putMsgReq)
		helpers.FatalIfError(err)

		// Retrieve value from the response.
		fmt.Println(putMsgResp)

	}

}

func getOrCreateStream() {

}
