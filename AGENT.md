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

## Impression Update (LLM)
- Update mode: `Update.update_mode` supports:
  - `per_user`: per user prompt/update.
  - `group_batch`: batch multiple users in one prompt.
  - `hybrid`: run both.
- Group batch prompt includes:
  - Existing profiles JSON keyed by `user_id`.
  - Known users list and id->nickname mapping for cross-target inference.
  - New messages grouped by `speaker_id` and raw text (including @id and reply_to).
- Optional semantic attribution step maps messages to target user_id (even without @/昵称).
  - Config: `Update.group_batch_enable_semantic_attribution`
  - Batch size cap is `Update.update_msg_threshold` (used as max messages per run).
- Writeback:
  - Only users present in LLM output are updated.
  - All messages included in the prompt are deleted from `message_queue`.
 - Force update:
   - If `Update.update_mode` is `group_batch`/`hybrid`, the force command triggers a single group batch update and clears included messages.
