module github.com/gopherex/xprobe/pkg/transport/grpc

go 1.25.0

replace github.com/gopherex/xprobe => ../../..

require (
	github.com/gopherex/xprobe v1.0.0
	google.golang.org/grpc v1.81.1
)

require (
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
