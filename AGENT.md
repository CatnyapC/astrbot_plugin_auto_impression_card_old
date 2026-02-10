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
- Data passed to LLM: a batched list of `alias_queue` records rendered as:
  - `"{idx}. [{YYYY-MM-DD HH:MM:SS}] speaker={speaker_id} text={message}"`
- `message` is built from the original event components as a single line:
  - `Plain` text chunks are joined with spaces.
  - `At` is rendered as `@<qq>`.
  - `Reply` is rendered as `[reply_to:<sender_id>]`.
- Filtering before enqueue:
  - Must include a non-bot target (`@id` or `[reply_to:id]`).
  - Drop messages that reply to the bot itself.
  - Drop commands starting with `/` or `／`.
- Trigger:
  - Auto analysis runs when `alias_queue` size >= `Alias.alias_analysis_batch_size`.
  - Admin command `称呼分析` runs immediately on the current queue.
- Writeback:
  - LLM returns JSON with `aliases` list.
  - Valid entries are upserted into `alias_map` with confidence (0.5-0.95).
  - On success, queued messages are deleted.
