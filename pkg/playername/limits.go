// Package playername holds the cross-service wire limits for player-name display resolution.
package playername

// ResolveBatchLimit is the maximum raw player_ids length accepted by ResolvePlayerNames.
// The limit is applied before de-duplication to prevent duplicate-input amplification.
const ResolveBatchLimit = 32
