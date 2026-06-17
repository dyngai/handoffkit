package runtime

import (
	"context"
	"unicode/utf8"

	"github.com/dyngai/handoffkit/sketch"
)

// WorkingState is an agent's full output for one step before it is projected
// onto a handoff: the complete prose Output plus the transcript Thread. It is
// the thing that does NOT fit on a channel (an approximation of the context
// window); the Compactor turns it into the lossy HandoffContext that does.
type WorkingState struct {
	Output string
	Thread []sketch.Turn
}

// CompactPolicy bounds a handoff. MaxSummaryBytes caps the prose Summary;
// KeepThreadTurns is how many trailing Thread turns survive verbatim. A
// MaxSummaryBytes <= 0 drops the Summary entirely (refs-only handoff); a
// KeepThreadTurns <= 0 drops the Thread.
type CompactPolicy struct {
	MaxSummaryBytes int
	KeepThreadTurns int
}

// Compactor projects a WorkingState onto a bounded HandoffContext. It writes the
// FULL output to a Corpus under the caller-supplied ref and returns a handoff
// carrying only a bounded Summary, the last KeepThreadTurns turns, and a
// MemoryRef pointing at the full text. That is the repo's answer to its headline
// open problem: the handoff stays small and bounded, yet the detail dropped from
// the Summary stays resolvable via the ref (Corpus.Get), instead of either being
// lost (over-summarized) or re-sent every hop (full output grows the prompt).
//
// summarize is the prose-shrinking step; a production agent would use an LLM. The
// default (nil) is deterministic head truncation, enough to make the bound
// measurable in tests without a model call.
type Compactor struct {
	corpus    sketch.Corpus
	policy    CompactPolicy
	summarize func(string) string
}

// NewCompactor builds a Compactor that offloads to corpus under policy. If
// summarize is nil, the Summary is the head of the output truncated to
// policy.MaxSummaryBytes (on a rune boundary) with a marker pointing at the
// corpus.
func NewCompactor(corpus sketch.Corpus, policy CompactPolicy, summarize func(string) string) *Compactor {
	return &Compactor{corpus: corpus, policy: policy, summarize: summarize}
}

// Compact writes ws.Output to the corpus at ref and returns a bounded handoff
// that references it. The returned Summary is guaranteed to be at most
// policy.MaxSummaryBytes bytes; Refs always includes ref so the receiver can
// recover the full output.
func (c *Compactor) Compact(ctx context.Context, ref sketch.MemoryRef, ws WorkingState) (sketch.HandoffContext, error) {
	if c.corpus == nil {
		return sketch.HandoffContext{}, errNilCorpus
	}
	// The full output is the system of record: store it once, reference it
	// thereafter. The ref is single-writer per handoff, so LastWriteWins (the
	// Corpus default) is the right policy for this key.
	if err := c.corpus.Merge(ctx, ref, ws.Output); err != nil {
		return sketch.HandoffContext{}, err
	}

	summary := ws.Output
	if c.summarize != nil {
		summary = c.summarize(summary)
	}
	summary = headTruncate(summary, c.policy.MaxSummaryBytes)

	var thread []sketch.Turn
	if c.policy.KeepThreadTurns > 0 && len(ws.Thread) > 0 {
		keep := c.policy.KeepThreadTurns
		if keep > len(ws.Thread) {
			keep = len(ws.Thread)
		}
		// Copy the tail so the handoff does not alias the caller's slice.
		thread = append(thread, ws.Thread[len(ws.Thread)-keep:]...)
	}

	return sketch.HandoffContext{
		Summary: summary,
		Thread:  thread,
		Refs:    []sketch.MemoryRef{ref},
	}, nil
}

// errNilCorpus is returned by Compact when the Compactor has no Corpus to
// offload to.
var errNilCorpus = compactError("handoffkit: compactor has a nil corpus")

type compactError string

func (e compactError) Error() string { return string(e) }

const truncMarker = " ...[truncated; full text in corpus]"

// headTruncate returns the head of s bounded to maxBytes bytes, cut on a rune
// boundary. When it truncates it appends a marker noting the full text is in the
// corpus (dropping the marker only if maxBytes is too small to fit it).
func headTruncate(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	budget := maxBytes - len(truncMarker)
	if budget <= 0 {
		return truncRunes(s, maxBytes) // no room for the marker; hard cut
	}
	return truncRunes(s, budget) + truncMarker
}

// truncRunes returns s cut to at most maxBytes bytes without splitting a rune.
func truncRunes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	// Back up off any continuation byte so the cut lands on a rune start.
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes]
}
