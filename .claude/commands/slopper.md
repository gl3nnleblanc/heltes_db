Read TODO.md and CLAUDE.md. Pick the single most natural next task from TODO.md — prefer tasks that build on what already exists without requiring other incomplete tasks as prerequisites. Use your judgement about logical sequencing.

Then implement the task strictly following the three-phase workflow in CLAUDE.md:

**Phase 1 — TLA+ specification**
Update `spec/HeltesDB.tla` to model the change. Verify the spec is internally consistent. Generate a representative set of concrete execution traces covering normal paths, edge cases, and error paths. These traces will drive Phase 2.

**Phase 2 — Test-driven development**
Write tests derived directly from the Phase 1 traces. Start with the bare minimum skeleton to make the tests compile, then iterate until the full test corpus passes. Do not write implementation logic beyond what is needed to pass the tests.

**Phase 3 — Implementation**
Complete the implementation. All tests must pass throughout. Do not introduce behaviour not covered by the spec.

Once the work is complete:
1. Remove the completed task from TODO.md
2. Commit all changes with a descriptive message
3. Push to main
