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

	ossStream := getOrCreateStream()

	putMsgInStream(*ossStream.MessagesEndpoint, *ossStream.Id)

}

func getMsgWithGroupCursor(streamEndpoint string, streamOcid string) {
	client, err := streaming.NewStreamClientWithConfigurationProvider(common.DefaultConfigProvider(), streamEndpoint)
	helpers.FatalIfError(err)

	// Create a request and dependent object(s).

	grpCursorCreateReq := streaming.CreateGroupCursorRequest{OpcRequestId: common.String("UR3R29BONB3MB5GP1KAC/OpcRequestIdExample/<unique_ID>"),
		StreamId: common.String("ocid1.test.oc1..<unique_ID>EXAMPLE-streamId-Value"),
		CreateGroupCursorDetails: streaming.CreateGroupCursorDetails{Type: streaming.CreateGroupCursorDetailsTypeTrimHorizon,
			CommitOnGet:  common.Bool(true),
			GroupName:    common.String("EXAMPLE-groupName-Value"),
			InstanceName: common.String("EXAMPLE-instanceName-Value"),
			Time:         &common.SDKTime{Time: time.Now()},
			TimeoutInMs:  common.Int(746)}}

	// Send the request using the service client
	resp, err := client.CreateGroupCursor(context.Background(), grpCursorCreateReq)
	helpers.FatalIfError(err)

	// Retrieve value from the response.
	fmt.Println(resp)
}

func putMsgInStream(streamEndpoint string, streamOcid string) {
	fmt.Println("Stream endpoint for put msg api is: " + streamEndpoint)
	streamClient, err := streaming.NewStreamClientWithConfigurationProvider(common.DefaultConfigProvider(), streamEndpoint)
	helpers.FatalIfError(err)

	// Create a request and dependent object(s).
	for i := 0; i < 5; i++ {
		putMsgReq := streaming.PutMessagesRequest{StreamId: common.String(streamOcid),
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

func getOrCreateStream() streaming.Stream {
	streamAdminClient, err := streaming.NewStreamAdminClientWithConfigurationProvider(common.DefaultConfigProvider())
	helpers.FatalIfError(err)
	cmptID := "ocid1.tenancy.oc1..aaaaaaaaopbu45aomik7sswe4nzzll3f6ii6pipd5ttw4ayoozez37qqmh3a"

	// Create a request and dependent object(s).

	createStreamPoolReq := streaming.CreateStreamPoolRequest{
		CreateStreamPoolDetails: streaming.CreateStreamPoolDetails{Name: common.String("streampool-go-example"),
			CompartmentId: common.String(cmptID)},
		OpcRequestId:    new(string),
		OpcRetryToken:   new(string),
		RequestMetadata: common.RequestMetadata{},
	}

	// Send the request using the service client
	streampoolRsp, err := streamAdminClient.CreateStreamPool(context.Background(), createStreamPoolReq)

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

	return streamRsp.Stream

}
