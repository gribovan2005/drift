---
type: process
status: stable
---

# Development Workflow (AI-Driven)

Every non-trivial change follows this sequence. The goal: deterministic agent output by providing precise contracts before code is written.

---

## Sequence

```
1. Spec  →  2. Code  →  3. Tests  →  4. PR
```

### 1. Spec first

Before writing any code, write or update the spec in `specs/` **and** the corresponding note in this vault.

The spec must answer:
- **What problem** does this solve?
- **Contract**: exact inputs, outputs, error conditions
- **Invariants**: things that must always be true (the agent must not violate these)
- **Testing requirements**: which tests are required before this is considered done

A spec without invariants is incomplete.

### 2. Code

Implement according to the spec. The spec is the source of truth — if code and spec conflict, fix the code.

Reference: [[Conventions]]

### 3. Tests

No PR ships without `go test ./...` passing. See [[Testing]] for what's required at each level.

### 4. PR

The PR description should reference the spec. Title = what changed. Body = why.

---

## Rules for AI agents

When implementing a feature:

1. **Read the spec first** — the relevant note in this vault + the corresponding file in `specs/`
2. **Check invariants** — list them before writing code; confirm each is upheld
3. **Check import rules** — `pkg/core` never imports other `pkg/` packages; see [[Index#Module layout]]
4. **Write tests alongside code** — not after; the test list is in the spec
5. **Don't add unrequested features** — implement exactly the spec, no extras

When modifying an existing component:
1. Update the spec first, then the code
2. Run `go test ./...` with `-race` flag
3. Run benchmarks if touching a hot path; check [[Testing#Regression gate]]

---

## When to update this vault

- Any new component → new note in `Specs/`
- Any design decision that surprised you → add to the relevant spec's "Why" section
- Any invariant violated in the past → add to the spec's "Invariants" so it can't happen again

The vault is a **memory of decisions**, not just a description of the current state.

---

## See also

- [[Conventions]]
- [[Testing]]
- [[Index]]
