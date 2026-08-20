// Package playerno holds the cross-service wire limits for player_no display resolution.
package playerno

// ResolveBatchLimit is the maximum raw player_ids length accepted by ResolvePlayerNos.
// The limit is applied before de-duplication to prevent duplicate-input amplification.
const ResolveBatchLimit = 32
