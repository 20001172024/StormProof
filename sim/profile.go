package sim

import "fmt"

// FaultConfig controls crashes and partitions. Every Interval the fault
// scheduler rolls the dice once for a crash and once for a partition.
type FaultConfig struct {
	Interval Time `json:"interval"` // 0 disables crashes and partitions

	CrashProb        float64 `json:"crash_prob"`         // chance of crashing a random running node
	MachineCrashProb float64 `json:"machine_crash_prob"` // fraction of crashes that also lose unsynced disk data
	MinDowntime      Time    `json:"min_downtime"`
	MaxDowntime      Time    `json:"max_downtime"`

	PartitionProb float64 `json:"partition_prob"` // chance of starting a partition, if none is active
	MinPartition  Time    `json:"min_partition"`
	MaxPartition  Time    `json:"max_partition"`
}

// Profile is a named set of fault settings.
type Profile struct {
	Name   string        `json:"name"`
	Net    NetworkConfig `json:"net"`
	Disk   DiskConfig    `json:"disk"`
	Faults FaultConfig   `json:"faults"`
	// ClockSkew makes each node's tick interval differ from the base by up
	// to this fraction (0.05 = ±5%), so nodes' timers drift apart the way
	// real clocks do.
	ClockSkew float64 `json:"clock_skew"`
}

var calmNet = NetworkConfig{MinDelay: 1 * Millisecond, MaxDelay: 5 * Millisecond}

// Profiles are the built-in fault profiles, from gentle to harsh.
var Profiles = []Profile{
	{
		Name: "calm",
		Net:  calmNet,
	},
	{
		Name:      "flaky-net",
		Net:       NetworkConfig{MinDelay: 1 * Millisecond, MaxDelay: 50 * Millisecond, DropProb: 0.1, DuplicateProb: 0.05},
		ClockSkew: 0.05,
	},
	{
		Name: "crashes",
		Net:  calmNet,
		Disk: DiskConfig{SyncFailProb: 0.002, SlowSyncProb: 0.05, SlowSyncMax: 20 * Millisecond},
		Faults: FaultConfig{
			Interval:  100 * Millisecond,
			CrashProb: 0.1, MachineCrashProb: 0.5,
			MinDowntime: 50 * Millisecond, MaxDowntime: 1 * Second,
		},
	},
	{
		Name: "partitions",
		Net:  calmNet,
		Faults: FaultConfig{
			Interval:      100 * Millisecond,
			PartitionProb: 0.1, MinPartition: 100 * Millisecond, MaxPartition: 2 * Second,
		},
	},
	{
		Name:      "chaos",
		Net:       NetworkConfig{MinDelay: 1 * Millisecond, MaxDelay: 50 * Millisecond, DropProb: 0.1, DuplicateProb: 0.05},
		Disk:      DiskConfig{SyncFailProb: 0.002, SlowSyncProb: 0.05, SlowSyncMax: 20 * Millisecond},
		ClockSkew: 0.1,
		Faults: FaultConfig{
			Interval:  100 * Millisecond,
			CrashProb: 0.1, MachineCrashProb: 0.5,
			MinDowntime: 50 * Millisecond, MaxDowntime: 1 * Second,
			PartitionProb: 0.1, MinPartition: 100 * Millisecond, MaxPartition: 2 * Second,
		},
	},
}

// HeavyProfiles are harsher than any of Profiles, for the nightly runs.
// They are kept out of Profiles (and so out of "-profile all") so the
// per-commit sweep stays the same size and speed.
var HeavyProfiles = []Profile{
	{
		// storm is harsher than chaos: twice the loss, duplication, crashes
		// and partitions, longer outages and partitions, five times the
		// fsync failures and twice the clock skew.
		//
		// Delays stay at most 50 ms, as in chaos, so a round trip is at most
		// the minimum election timeout (100 ms with the default 10 ticks of
		// 10 ms). Raft promises progress only while round trips are well
		// under the election timeout (§5.6); at 100 ms delays some runs end
		// without a leader however correct the code is (docs/bugs/005).
		// Partitions last at most 3 s, under the default 5 s quiet period.
		Name:      "storm",
		Net:       NetworkConfig{MinDelay: 1 * Millisecond, MaxDelay: 50 * Millisecond, DropProb: 0.2, DuplicateProb: 0.1},
		Disk:      DiskConfig{SyncFailProb: 0.01, SlowSyncProb: 0.1, SlowSyncMax: 50 * Millisecond},
		ClockSkew: 0.2,
		Faults: FaultConfig{
			Interval:  100 * Millisecond,
			CrashProb: 0.2, MachineCrashProb: 0.5,
			MinDowntime: 50 * Millisecond, MaxDowntime: 2 * Second,
			PartitionProb: 0.2, MinPartition: 100 * Millisecond, MaxPartition: 3 * Second,
		},
	},
}

// ProfileByName returns the built-in profile with the given name.
func ProfileByName(name string) (Profile, error) {
	for _, p := range append(Profiles[:len(Profiles):len(Profiles)], HeavyProfiles...) {
		if p.Name == name {
			return p, nil
		}
	}
	return Profile{}, fmt.Errorf("unknown fault profile %q", name)
}
