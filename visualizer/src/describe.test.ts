import { describe, expect, it } from "vitest";
import { summarizeMessage } from "./describe";

describe("summarizeMessage", () => {
  it("describes each Raft message in Figure 2's terms", () => {
    expect(summarizeMessage("PreVote", { term: 4, last_log_index: 9, last_log_term: 3 })).toBe("would campaign in term 4, last log 9/3");
    expect(summarizeMessage("PreVoteResponse", { term: 4, vote_granted: true })).toBe("would vote in term 4");
    expect(summarizeMessage("PreVoteResponse", { term: 6 })).toBe("no (in term 6)");
    expect(summarizeMessage("RequestVoteResponse", { term: 2 })).toBe("term 2, denied");
    expect(summarizeMessage("AppendEntries", { term: 2, entries: [{}, {}], prev_log_index: 5, prev_log_term: 1, leader_commit: 4 })).toBe(
      "term 2, 2 entries, prev 5/1, commit 4",
    );
    expect(summarizeMessage("AppendEntries", { term: 2 })).toBe("term 2, heartbeat, prev 0/0, commit 0");
    expect(summarizeMessage("AppendEntriesResponse", { term: 2, conflict_index: 3, conflict_term: 1 })).toBe("term 2, rejected, conflict 3/1");
    expect(summarizeMessage("ClientRequest", { cmd: { op: "put", key: "k1", request: 3 } })).toBe("put k1 (req 3)");
  });
});
