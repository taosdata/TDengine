package run

import (
	"testing"

	"tdsqlsmith/internal/taosdwatch"
)

// TestShouldRecordTaosdCrashWithParentChildModel verifies crash detection using
// parent-child process model evidence (from managed exit metadata)
func TestShouldRecordTaosdCrashWithParentChildModel(t *testing.T) {
	testCases := []struct {
		name     string
		incident taosdwatch.Incident
		expect   bool
	}{
		{
			name: "crash signal with coredump detected via managed exit",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
			},
			expect: true,
		},
		{
			name: "crash signal SIGSEGV via managed exit",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal SIGSEGV (core_dump=true)",
			},
			expect: true,
		},
		{
			name: "crash signal aborted via managed exit",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal aborted (core_dump=true)",
			},
			expect: true,
		},
		{
			name: "crash signal SIGABRT via managed exit",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal SIGABRT (core_dump=true)",
			},
			expect: true,
		},
		{
			name: "crash signal bus error via managed exit",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal bus error (core_dump=true)",
			},
			expect: true,
		},
		{
			name: "coredump detected but not from managed taosd exit (old filesystem format)",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "recent core file: /tmp/core.taosd.12345",
			},
			expect: false, // Parent-child model requires "managed taosd exited" format
		},
		{
			name: "coredump detected with generic evidence",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "recent core file: /tmp/core.12345",
			},
			expect: false,
		},
		{
			name: "no coredump detected",
			incident: taosdwatch.Incident{
				CoredumpDetected: false,
				ExitReason:       "managed_taosd_exit exit_code=0",
			},
			expect: false,
		},
		{
			name: "unknown 65535 runtime error without coredump",
			incident: taosdwatch.Incident{
				CoredumpDetected: false,
				Error:            "Unknown error 65535",
			},
			expect: true,
		},
		{
			name: "empty coredump evidence",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "",
			},
			expect: false,
		},
		{
			name: "coredump evidence without managed taosd prefix",
			incident: taosdwatch.Incident{
				CoredumpDetected: true,
				CoredumpEvidence: "some other crash info",
			},
			expect: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := shouldRecordTaosdCrash(tc.incident)
			if result != tc.expect {
				t.Errorf("shouldRecordTaosdCrash() = %v, want %v\nincident: %+v",
					result, tc.expect, tc.incident)
			}
		})
	}
}

// TestIsTaosdCoredumpEvidence verifies the helper function for checking evidence format
func TestIsTaosdCoredumpEvidence(t *testing.T) {
	testCases := []struct {
		name     string
		evidence string
		expect   bool
	}{
		{
			name:     "managed taosd exit with signal",
			evidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
			expect:   true,
		},
		{
			name:     "managed taosd exit with SIGSEGV uppercase",
			evidence: "managed taosd exited by signal SIGSEGV",
			expect:   true,
		},
		{
			name:     "managed taosd exit uppercase",
			evidence: "MANAGED TAOSD EXITED BY SIGNAL",
			expect:   true,
		},
		{
			name:     "old filesystem format",
			evidence: "recent core file: /tmp/core.taosd.12345",
			expect:   false,
		},
		{
			name:     "empty evidence",
			evidence: "",
			expect:   false,
		},
		{
			name:     "whitespace only",
			evidence: "   ",
			expect:   false,
		},
		{
			name:     "unrelated text",
			evidence: "some random crash information",
			expect:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isTaosdCoredumpEvidence(tc.evidence)
			if result != tc.expect {
				t.Errorf("isTaosdCoredumpEvidence(%q) = %v, want %v",
					tc.evidence, result, tc.expect)
			}
		})
	}
}
