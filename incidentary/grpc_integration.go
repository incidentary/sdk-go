package incidentary

import "context"

// GRPCIntegration provides gRPC instrumentation helpers through the standard
// Integration interface.
//
// Because google.golang.org/grpc is not a dependency of this module, gRPC
// instrumentation is provided as context-level helpers that callers wire into
// their own unary/streaming interceptors.
//
//	reg := incidentary.NewIntegrationRegistry(client)
//	grpcInt := &incidentary.GRPCIntegration{}
//	reg.Register(grpcInt)
//	reg.SetupAll()
//
//	// Server-side unary interceptor:
//	func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
//	    md, _ := metadata.FromIncomingContext(ctx)
//	    carrier := incidentary.GRPCMetadataCarrier{}
//	    for k, vs := range md {
//	        if len(vs) > 0 { carrier[k] = vs[0] }
//	    }
//	    wrapped := grpcInt.ExtractAndHandle(carrier, func(ctx context.Context) error {
//	        resp, err = handler(ctx, req)
//	        return err
//	    })
//	    return resp, wrapped(ctx)
//	}
type GRPCIntegration struct {
	client *Client
}

// Name returns the integration identifier.
func (g *GRPCIntegration) Name() string {
	return "grpc"
}

// Setup stores the client reference so InjectMetadata and ExtractAndHandle
// can use it. It performs no global side effects and returns a no-op cleanup.
func (g *GRPCIntegration) Setup(client *Client) (func(), error) {
	g.client = client
	return nil, nil
}

// InjectMetadata injects Incidentary trace context from ctx into a new copy
// of md, ready to attach to outgoing gRPC call metadata. The original md map
// is never mutated.
func (g *GRPCIntegration) InjectMetadata(ctx context.Context, md GRPCMetadataCarrier) GRPCMetadataCarrier {
	return InjectGRPCContext(ctx, md)
}

// ExtractAndHandle wraps a gRPC handler function with trace context extraction
// from md and records a grpc_in event after the handler returns. If Setup has
// not been called (client is nil), the handler is still called but no event is
// recorded.
func (g *GRPCIntegration) ExtractAndHandle(md GRPCMetadataCarrier, handler func(ctx context.Context) error) func(ctx context.Context) error {
	return WrapGRPCHandler(g.client, md, handler)
}
