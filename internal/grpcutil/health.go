// Package grpcutil holds small helpers shared by the gRPC middleware layers.
package grpcutil

import "strings"

// healthMethodPrefix matches grpc.health.v1.Health/Check, /Watch and /List.
const healthMethodPrefix = "/grpc.health.v1.Health/"

// IsHealthMethod reports whether fullMethod is a gRPC health-checking call.
//
// Health RPCs are exempt from both authentication (Kubernetes gRPC probes
// can't present a bearer token) and rate limiting (a probe that gets
// ResourceExhausted during a load spike fails, and the kubelet restarts a pod
// that was healthy — load shedding must never turn into a restart loop).
func IsHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, healthMethodPrefix)
}
