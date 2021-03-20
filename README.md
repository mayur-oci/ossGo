
# Quickstart with OCI Go SDK for OSS

This quickstart shows how to produce messages to and consume messages from an **Oracle Streaming Service**{oss docs link @jb} using the OCI Go SDK{github link @jb}.

## Prerequisites

1. You need have OCI account subscription or free account. typical links @jb
2. Follow [these steps](https://github.com/mayur-oci/OssJs/blob/main/JavaScript/CreateStream.md) to create Streampool and Stream in OCI. If you do  already have stream created, refer step 3 [here](https://github.com/mayur-oci/OssJs/blob/main/JavaScript/CreateStream.md) to capture/record message endpoint and OCID of the stream. We need this Information for upcoming steps.
3. **Go** installed locally. Follow  [these instructions](https://golang.org/doc/install)  if necessary. Make sure **go** is in your **PATH**.
4. Visual Studio Code(recommended) or any other integrated development environment (IDE) or text editor.
6. Make sure you have [SDK and CLI Configuration File](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm#SDK_and_CLI_Configuration_File) setup. For production, you should use [Instance Principle Authentication](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm).

## Producing messages to OSS
1. Open your favorite editor, such as [Visual Studio Code](https://code.visualstudio.com) from empty directory say *wd*. 
2. Create new file named *Producer.go* in this directory and paste the following code in it. Change the values constants namely *ociMessageEndpoint, ociStreamOcid, ociConfigFilePath and ociProfileName*, as per your environment. Save the file editor you are using.
```Go
package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/oracle/oci-go-sdk/v36/common"
	"github.com/oracle/oci-go-sdk/v36/example/helpers"
	"github.com/oracle/oci-go-sdk/v36/streaming"
)

const ociMessageEndpoint = "https://cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com"
const ociStreamOcid = "ocid1.stream.oc1.ap-mumbai-1.amaaaaaauwpiejqaxcfc2ht67wwohfg7mxcstfkh2kp3hweeenb3zxtr5khq"
const ociConfigFilePath = "C:\\.oci\\config"
const ociProfileName = "DEFAULT"

func main() {
	fmt.Println("Go oci oss sdk example producer")
	putMsgInStream(ociMessageEndpoint, ociStreamOcid)
}

func putMsgInStream(streamEndpoint string, streamOcid string) {
	fmt.Println("Stream endpoint for put msg api is: " + streamEndpoint)

	provider, err := common.ConfigurationProviderFromFileWithProfile(ociConfigFilePath, ociProfileName, "")
	helpers.FatalIfError(err)

	streamClient, err := streaming.NewStreamClientWithConfigurationProvider(provider, streamEndpoint)
	helpers.FatalIfError(err)

	// Create a request and dependent object(s).
	for i := 0; i < 5; i++ {
		putMsgReq := streaming.PutMessagesRequest{StreamId: common.String(streamOcid),
			PutMessagesDetails: streaming.PutMessagesDetails{
				// we are batching 2 messages for each Put Request
				Messages: []streaming.PutMessagesDetailsEntry{
					{Key: []byte("key dummy-0-" + strconv.Itoa(i)),
						Value: []byte("value dummy-" + strconv.Itoa(i))},
					{Key: []byte("key dummy-1-" + strconv.Itoa(i)),
						Value: []byte("value dummy-" + strconv.Itoa(i))}}},
		}

		// Send the request using the service client
		putMsgResp, err := streamClient.PutMessages(context.Background(), putMsgReq)
		helpers.FatalIfError(err)

		// Retrieve value from the response.
		fmt.Println(putMsgResp)
	}

}
```
3. Open terminal. Change the working directory to the same directory *wd*. Execute the following commands from the terminal now, one after the other.
```
#this command creates go.mod file in the wd directory
go mod init oss_producer_example/v0 

#this will install OCI Go SDK for OSS
go mod tidy 

go run Consumer.go
```

5. In the OCI Web Console, quickly go to your Stream Page and click on *Load Messages* button. You should see the messages we just produced similar to  below screenshot.
![See Produced Messages in OCI Wb Console](https://github.com/mayur-oci/OssJs/blob/main/JavaScript/StreamExampleLoadMessages.png?raw=true)

  
## Consuming messages from OSS
1. First produce messages to the stream you want to consumer message from unless you already have messages in the stream. You can produce message easily from *OCI Web Console* using simple *Produce Test Message* button as shown below
![Produce Test Message Button](https://github.com/mayur-oci/OssJs/blob/main/JavaScript/ProduceButton.png?raw=true)
 
 You can produce multiple test messages by clicking *Produce* button back to back, as shown below
![Produce multiple test message by clicking Produce button](https://github.com/mayur-oci/OssJs/blob/main/JavaScript/ActualProduceMessagePopUp.png?raw=true)
2. Open your favorite editor, such as [Visual Studio Code](https://code.visualstudio.com) from the directory say *wd*. 
3.  Create new file named *Consumer.go* in this directory and paste the following code in it. Change the values constants namely *ociMessageEndpoint, ociStreamOcid, ociConfigFilePath and ociProfileName*, as per your environment. Save the file editor you are using.
```Go
package main

import (
	"context"
	"fmt"

	"github.com/oracle/oci-go-sdk/v36/common"
	"github.com/oracle/oci-go-sdk/v36/example/helpers"
	"github.com/oracle/oci-go-sdk/v36/streaming"
)

const ociMessageEndpoint = "https://cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com"
const ociStreamOcid = "ocid1.stream.oc1.ap-mumbai-1.amaaaaaauwpiejqaxcfc2ht67wwohfg7mxcstfkh2kp3hweeenb3zxtr5khq"
const ociConfigFilePath = "C:\\.oci\\config"
const ociProfileName = "DEFAULT"

func main() {
	fmt.Println("Go oci oss sdk example for consumer")
	getMsgWithGroupCursor(ociMessageEndpoint, ociStreamOcid)
}

func getMsgWithGroupCursor(streamEndpoint string, streamOcid string) {
	client, err := streaming.NewStreamClientWithConfigurationProvider(common.DefaultConfigProvider(), streamEndpoint)
	helpers.FatalIfError(err)

	grpCursorCreateReq0 := streaming.CreateGroupCursorRequest{
		StreamId: common.String(streamOcid),
		CreateGroupCursorDetails: streaming.CreateGroupCursorDetails{Type: streaming.CreateGroupCursorDetailsTypeTrimHorizon,
			CommitOnGet:  common.Bool(true),
			GroupName:    common.String("Go-groupname-0"),
			InstanceName: common.String("Go-groupname-0-instancename-0"),
			TimeoutInMs:  common.Int(1000),
		}}

	// Send the request using the service client
	grpCursorResp0, err := client.CreateGroupCursor(context.Background(), grpCursorCreateReq0)
	helpers.FatalIfError(err)
	// Retrieve value from the response.
	fmt.Println(grpCursorResp0)

	simpleGetMsgLoop(client, streamOcid, *grpCursorResp0.Value)
}

func simpleGetMsgLoop(streamClient streaming.StreamClient, streamOcid string, cursorValue string) {

	for i := 0; i < 5; i++ {
		getMsgReq := streaming.GetMessagesRequest{Limit: common.Int(3),
			StreamId: common.String(streamOcid),
			Cursor:   common.String(cursorValue)}

		// Send the request using the service client
		getMsgResp, err := streamClient.GetMessages(context.Background(), getMsgReq)
		helpers.FatalIfError(err)

		// Retrieve value from the response.
		if len(getMsgResp.Items) > 0 {
			fmt.Println("Key : " + string(getMsgResp.Items[0].Key) + ", value : " + string(getMsgResp.Items[0].Value) + ", Partition " + *getMsgResp.Items[0].Partition)
		}
		if len(getMsgResp.Items) > 1 {
			fmt.Println("Key : " + string(getMsgResp.Items[1].Key) + ", value : " + string(getMsgResp.Items[1].Value) + ", Partition " + *getMsgResp.Items[1].Partition)
		}
		cursorValue = *getMsgResp.OpcNextCursor

	}
}


```
4. Open the terminal. Change the working directory to the same directory *wd*. Execute the following commands from the terminal now, one after the other.
```
#this command creates go.mod file in the wd directory
go mod init oss_consumer_example/v0 

#this will install OCI Go SDK for OSS
go mod tidy 

go run Consumer.go
```
5. You should see the messages as shown below. Note when we produce message from OCI Web Console(as described above in first step), the Key for each message is *Null*.
```
$:/path/to/directory/wd>go run Consumer.go
Go oci oss sdk example for consumer
{ RawResponse={200 OK 200 HTTP/1.1 1 1 map[Access-Control-Allow-Credentials:[true] Access-Control-Allow-Methods:[POST,PUT,GET,HEAD,DELETE,OPTIONS] Access-Control-Allow-Origin:[*] Access-Control-Expose-Headers:[access-control-allow-credentials,access-control-allow-methods,access-control-allow-origin,connection,content-length,content-type,opc-client-info,opc-request-id] Connection:[keep-alive] Content-Length:[432] Content-Type:[application/json] Opc-Request-Id:[07f00ea758075ca8ad9c9be649502a45/1B9AF2861AE7A05EC11BEBB662215868/1F05E6A5B860282188F223E3B085AE8A]] 0xc000214020 432 [] false false map[] 0xc00012c300 0xc0000f8420} Cursor={ Value=eyJjdXJzb3JUeXBlIjoiZ3JvdXAiLCJzdHJlYW1JZCI6Im9jaWQxLnN0cmVhbS5vYzEuYXAtbXVtYmFpLTEuYW1hYWFhYWF1d3BpZWpxYXhjZmMyaHQ2N3d3b2hmZzdteGNzdGZraDJrcDNod2VlZW5iM3p4dHI1a2hxIiwiZXhwaXJhdGlvbiI6MTYxNjE2NTY1ODQwMCwiZ3JvdXBOYW1lIjoiR28tZ3JvdXBuYW1lLTAiLCJpbnN0YW5jZU5hbWUiOiJHby1ncm91cG5hbWUtMC1pbnN0YW5jZW5hbWUtMCIsIm9mZnNldHMiOnt9LCJjb21taXRPbkdldCI6dHJ1ZSwiZ2VuZXJhdGlvbiI6MCwidGltZW91dEluTXMiOjEwMDAsImN1cnNvclR5cGUiOiJncm91cCJ9 } OpcRequestId=07f00ea758075ca8ad9c9be649502a45/1B9AF2861AE7A05EC11BEBB662215868/1F05E6A5B860282188F223E3B085AE8A }
Key : , value : Example Test Message 0, Partition 0
Key : , value : Example Test Message 0, Partition 0
Key : , value : Example Test Message 0, Partition 0
Key : , value : Example Test Message 0, Partition 0
Key : , value : Example Test Message 0, Partition 0

```

## Next Steps
Please refer to

 1. [Github for OCI Go SDK](https://github.com/oracle/oci-go-sdk)
