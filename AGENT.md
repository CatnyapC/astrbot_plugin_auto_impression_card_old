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
