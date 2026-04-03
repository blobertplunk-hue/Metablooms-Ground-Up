# MPP Stage Pipeline (Runtime Enforced)

Runtime enforcement order (cannot skip):
1. SEE Gate
2. Problem Formalization
3. Multi-Option Generation
4. Evaluation Matrix
5. Decision Record
6. Missing Middle Detector
7. Implementation Plan
8. Implementation
9. Validation (must use invariant registry / existing guardrails)
10. Refinement Loop

Artifacts are rooted at `mpp_artifacts/<task_id>/` and validated by `src/mpp_stage_pipeline.py`.
