//! The transcript rows Claude writes for an interruption, a compaction and a
//! turn's end, as the harness evidence (flows/6fa457/harness-evidence/claude.md,
//! 4a17 and 8hpf) and Claude 2.1.323's own transcripts record them, and how the
//! translator reads them.

use claude_harness_process::Row;
use claude_harness_translator::Translatable;
use harness::{Compaction, Tokens, Trigger};

/// 8hpf: Escape sent while a Bash call ran.
const INTERRUPTED_FOR_TOOL_USE: &str = r#"{"parentUuid":"758cf75b-58c2-445c-a4f0-23b55f04d19c","isSidechain":false,"promptId":"419847c6-2c73-4ab8-875b-b42247e1e0bf","type":"user","message":{"role":"user","content":[{"type":"text","text":"[Request interrupted by user for tool use]"}]},"uuid":"6cbd3db6-2121-4bee-aebf-d4e257b0ec91","timestamp":"2026-11-21T04:49:59.189Z","interruptedMessageId":"msg_011ChFUEHkgXmDm22XdX8ULw","session_id":"9c6908c4-5788-49e0-861c-eb98d9406fe3","userType":"external","entrypoint":"cli","cwd":"/tmp/claude-1001/-home-li-primary/4682ac0e-82eb-520f-bbaf-4365e9087c17/scratchpad/8hpf.Ru0d","sessionId":"9c6908c4-5788-49e0-861c-eb98d9406fe3","version":"2.1.325","gitBranch":"HEAD"}"#;

/// The same row when no tool was running, as Claude 2.1.325 wrote it in a headless run.
const INTERRUPTED: &str = r#"{"parentUuid":"af59bd64-9e58-4a4d-bdff-f2dce987cdca","isSidechain":false,"promptId":"c278f00d-3e89-4fc5-9adb-dedf2cf3d9f3","type":"user","message":{"role":"user","content":[{"type":"text","text":"[Request interrupted by user]"}]},"uuid":"532520d2-9548-4445-b003-05eddf9a28c4","timestamp":"2026-11-21T03:50:27.794Z","userType":"external","entrypoint":"sdk-cli","cwd":"/tmp/tmp.wh4gHVw8k5/work","sessionId":"becd525c-d0f8-4343-ad8e-16a92c835fac","version":"2.1.325","gitBranch":"HEAD"}"#;

/// 4a17: the compact_boundary row of the /compact run.
const COMPACT_BOUNDARY: &str = r#"{"parentUuid":null,"logicalParentUuid":"ebb874e8-0546-4e73-9b77-77f44e2533cc","isSidechain":false,"type":"system","subtype":"compact_boundary","content":"Conversation compacted","isMeta":false,"timestamp":"2026-11-21T04:46:23.665Z","uuid":"28f51cc5-c119-4da9-a58f-b8105fe6a01a","level":"info","compactMetadata":{"trigger":"manual","preTokens":18336,"durationMs":6239,"preservedSegment":{"headUuid":"378bfd43-3b9b-4afc-8434-0e3d64737af2","anchorUuid":"97dda91c-826e-4ac7-b817-936aa9845d71","tailUuid":"378bfd43-3b9b-4afc-8434-0e3d64737af2"},"preservedMessages":{"anchorUuid":"97dda91c-826e-4ac7-b817-936aa9845d71","uuids":["378bfd43-3b9b-4afc-8434-0e3d64737af2"],"allUuids":["378bfd43-3b9b-4afc-8434-0e3d64737af2","d7e8d413-6745-43ef-b448-d138c68acbac","ebb874e8-0546-4e73-9b77-77f44e2533cc"]},"postTokens":6002,"cumulativeDroppedTokens":12334},"userType":"external","entrypoint":"sdk-cli","cwd":"/tmp/claude-1001/-home-li-primary/4682ac0e-82eb-520f-bbaf-4365e9087c17/scratchpad/23cq.6xA1","sessionId":"e864fd14-abac-4cc3-9d5a-6f536041fc7b","version":"2.1.325","gitBranch":"HEAD","slug":"groovy-roaming-waterfall"}"#;

#[test]
fn the_user_row_of_an_escape_is_interrupted() {
    assert_eq!(Row::translate(INTERRUPTED_FOR_TOOL_USE), Ok(Row::Interrupted));
    assert_eq!(Row::translate(INTERRUPTED), Ok(Row::Interrupted));
}

#[test]
fn the_tool_result_that_comes_with_an_interruption_is_other() {
    let rejected = r#"{"type":"user","isSidechain":false,"message":{"role":"user","content":[{"type":"tool_result","content":"The user doesn't want to proceed with this tool use.","is_error":true,"tool_use_id":"toolu_01NQiedoM42SnpipnhDKDrVy"}]}}"#;
    assert_eq!(Row::translate(rejected), Ok(Row::Other));
}

#[test]
fn a_compact_boundary_is_compacted_with_its_trigger_and_size_before() {
    assert_eq!(Row::translate(COMPACT_BOUNDARY), Ok(Row::Compacted(Compaction { trigger: Trigger::Manual, tokens_option: Some(Tokens(18336)) })));
}

#[test]
fn an_automatic_compaction_says_so() {
    let automatic = COMPACT_BOUNDARY.replace(r#""trigger":"manual""#, r#""trigger":"auto""#);
    assert_eq!(Row::translate(&automatic), Ok(Row::Compacted(Compaction { trigger: Trigger::Automatic, tokens_option: Some(Tokens(18336)) })));
}

#[test]
fn a_compact_boundary_without_its_size_is_compacted_without_tokens() {
    let sizeless = COMPACT_BOUNDARY.replace(r#""preTokens":18336,"#, "");
    assert_eq!(Row::translate(&sizeless), Ok(Row::Compacted(Compaction { trigger: Trigger::Manual, tokens_option: None })));
}

#[test]
fn a_compact_boundary_of_a_trigger_not_known_is_other() {
    let unknown = COMPACT_BOUNDARY.replace(r#""trigger":"manual""#, r#""trigger":"hook""#);
    assert_eq!(Row::translate(&unknown), Ok(Row::Other));
}

/// The system row Claude 2.1.323 writes as every turn of an interactive session ends.
const TURN_DURATION: &str = r#"{"parentUuid":"c3b00c5c-9d92-4a79-a4dd-9254f20ce68b","isSidechain":false,"type":"system","subtype":"turn_duration","durationMs":1412,"messageCount":15,"timestamp":"2026-11-14T09:59:18.439Z","uuid":"ae334773-aa5c-4907-a829-e241ab572622","isMeta":false,"userType":"external","entrypoint":"cli","cwd":"/tmp/flow-testing/workspace-a","sessionId":"ba2d4c3e-d26c-53c4-b57d-375062952e2d","version":"2.1.323","gitBranch":"HEAD"}"#;

#[test]
fn a_turn_duration_row_ends_the_turn() {
    assert_eq!(Row::translate(TURN_DURATION), Ok(Row::TurnEnded));
}

#[test]
fn a_subagent_turn_duration_row_is_other() {
    let sidechain = TURN_DURATION.replace(r#""isSidechain":false"#, r#""isSidechain":true"#);
    assert_eq!(Row::translate(&sidechain), Ok(Row::Other));
}

#[test]
fn a_stop_hook_summary_row_is_other() {
    let summary = r#"{"parentUuid":"0b7f2a14-3c5e-4d0e-9f61-2a8c4e6d1b33","isSidechain":false,"type":"system","subtype":"stop_hook_summary","hookCount":1,"hookInfos":[{"command":"claude-harness-hook"}],"hookErrors":[],"preventedContinuation":false,"stopReason":"","hasOutput":false,"level":"suggestion","timestamp":"2026-11-21T04:50:02.101Z","uuid":"5d1e9c47-8a2b-4f36-b0c4-71e3a9f2d8e6","sessionId":"9c6908c4-5788-49e0-861c-eb98d9406fe3","version":"2.1.325"}"#;
    assert_eq!(Row::translate(summary), Ok(Row::Other));
}
