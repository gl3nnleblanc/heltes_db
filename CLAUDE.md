# Development Rules

## Workflow

## Task selection order

**The "Benchmarking & Validation" section of TODO.md is the current sprint.**
When picking the next task — for `/slopper` or otherwise — you MUST select from that section first.
Do not pick any task from any other section (Correctness, Performance, Durability, Operability, Protocol extensions, Other) while any Benchmarking & Validation item remains on the list.
Once that section is fully drained, remove this constraint and resume normal priority judgement.

---

All functional changes follow a strict three-phase process. Do not skip phases or work them out of order.

### Phase 1: TLA+ specification

Before writing any Rust code for a functional change:

1. Model the change in `spec/HeltesDB.tla`
2. Verify the updated spec with TLC — it must pass with zero errors
3. Use TLC's state graph to generate a large corpus of concrete execution traces (path equivalence classes, edge cases, error paths). These traces become the test cases for Phase 2.

Do not proceed to Phase 2 until the spec is clean and the trace corpus is in hand.

### Phase 2: Test-driven development

With the trace corpus from Phase 1:

1. Write tests derived directly from the TLC traces — each test encodes a concrete execution path the model checker explored
2. Implement the bare minimum skeleton to make the tests compile and run — not to make them pass
3. Iterate on the implementation until the full test corpus passes
4. No implementation code beyond what is needed to pass the tests

Do not add implementation logic that is not driven by a failing test.

### Phase 3: Implementation

Only after the spec is verified and the full test suite passes:

1. Fill out the implementation properly
2. All tests must continue to pass throughout
3. Do not introduce behaviour that is not covered by the spec
