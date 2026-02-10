# 称呼策略迭代 Plan

目标：默认启用“他人称呼迁移”，结合最近出现次数与优先级，选择更合适的称呼。

## 1. 数据来源与统计
- 记录来源：`message_queue`（raw，含 `@id` / `[reply_to:id]`）与 `alias_map`。
- 统计方法：
  - 从 `message_queue` 中检索最近 N 天的消息（按 group_id）。
  - 解析 raw 文本，统计每个 `(speaker_id, target_id, alias)` 的出现次数。
  - 与 `alias_map` 做交集过滤，仅保留 LLM 认可的 alias。
- 可配置项（建议）：
  - `alias_usage_window_days`（默认 14）
  - `alias_usage_min_count`（默认 2）

## 2. 选择优先级（默认启用）
1. 当前说话人对目标的别称（speaker → target）
2. 目标本人昵称/自称（profiles.nickname）
3. 他人对目标的别称（迁移使用）

## 3. 迁移策略（他人称呼）
- 只使用满足以下条件的别称：
  - 在最近 N 天出现次数 >= `alias_usage_min_count`
  - 位于 `alias_map` 的 top-K（当前 K=4）
- 若出现次数相同，按 `alias_map` confidence 作为次序（可选）
- 若无合格别称，回退到 `nickname`

## 4. 风险控制
- 加入“降级”机制：用户明确拒绝后，降低该称呼优先级
- 配置开关：
  - `enable_alias_migration`（默认 true）

## 5. 输出方式
- 先实现一个选择器函数：`pick_addressing_name(speaker_id, target_id, group_id)`
- 工具或内部调用统一使用此函数
