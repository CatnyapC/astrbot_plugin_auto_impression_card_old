# Auto Impression Card Plugin

## Debug Mode
- Config key: `Debug.debug_mode`
- When enabled, the plugin logs:
  - The full injected profile context string
  - The full LLM update prompt
  - The raw LLM response text

## Notes
- Platform scope: QQ (AIOCQHTTP) only.
- Paths: use `pathlib.Path` + `astrbot.core.utils.path_utils` for data dirs.

## Alias Analysis (LLM)
- Data passed to LLM: a batched list of `message_queue` records (raw format) rendered as:
  - `"{idx}. [{YYYY-MM-DD HH:MM:SS}] speaker={user_id} text={message}"`
- `message` (raw) is built from the original event components as a single line:
  - `Plain` text chunks are joined with spaces.
  - `At` is rendered as `@<qq>`.
  - `Reply` is rendered as `[reply_to:<sender_id>]`.
- Trigger:
  - Auto analysis runs when group-level pending messages >= `Alias.alias_analysis_batch_size`.
  - Admin command `称呼分析` runs immediately on current group messages.
- Writeback:
  - LLM returns JSON with `aliases` list.
  - Valid entries are upserted into `alias_map` with confidence (0.5-0.95).
  - Each (speaker_id, target_id) pair is pruned to top 4 aliases by confidence.

## Message Queue (Raw)
- `message_queue` now stores raw messages (with `@id` and `[reply_to:id]`).
- Plain-text filtering is only applied when plain text exists.
- Impression updates pass raw messages to LLM to preserve attribution cues.
- Analysis uses the current pending queue only; no extra time window/size cap
  beyond existing thresholds (e.g. `Update.update_msg_threshold`).

## Impression Update (LLM)
- Update mode: `Update.update_mode` supports:
  - `per_user`: per user prompt/update.
  - `group_batch`: batch multiple users in one prompt.
  - `hybrid`: run both.
- Four-step pipeline:
  1. Semantic attribution (message -> target user_id)
  2. Phase1 candidate extraction (traits/facts + evidence_ids)
  3. Phase2 merge/replace (final traits/facts + mapping)
  4. Phase3 summary update (summary only)
- Optional semantic attribution step maps messages to target user_id (even without @/昵称).
  - Config: `Update.group_batch_enable_semantic_attribution`
  - Batch size cap is `Update.update_msg_threshold` (used as max messages per run).
- Alias analysis is triggered before group updates to improve attribution.
- Bot alias fixed mapping:
  - `Basic.bot_user_id`
  - `Basic.bot_aliases`
- Model selection:
  - `Model.alias_provider_id` for alias analysis
  - `Model.attribution_provider_id` for attribution
  - `Model.phase1_provider_id` / `phase2_provider_id` / `phase3_provider_id` for phases
  - fall back to `Model.update_provider_id` then current session provider
- Alias confidence uses the same trust/decay formula and is computed from alias evidence.
- Evidence half-life: `Update.evidence_half_life_days`
- Global force update: `/印象更新` with `全体/全部/all/a` updates all known groups.
- Evidence storage:
  - `profiles.examples` is not used.
  - Evidence is stored in `impression_evidence` keyed by trait/fact.
- Confidence:
  - Phase 1 adds evidence-level confidence, joke likelihood, and source type.
  - Phase 2 adds consistency marker.
  - Final confidence computed via formula and stored per trait/fact.
  - Evidence confidence is recomputed from all stored evidence with half-life decay.
- Facts below `Update.fact_confidence_min` are dropped (and evidence removed).
- Writeback:
  - Only users present in LLM output are updated.
  - All messages included in the prompt are deleted from `message_queue`.
- Force update:
  - If `Update.update_mode` is `group_batch`/`hybrid`, the force command triggers a single group batch update and clears included messages.
  - Add keyword `全体`/`全部`/`all`/`a` to update all known groups.
