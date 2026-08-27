import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("node connection button performs a dedicated test and reports results", async () => {
  const source = await readFile(new URL("../app/page.tsx", import.meta.url), "utf8");

  assert.match(source, /const testNodeConnection = useCallback/);
  assert.match(source, /\/api\/jobs\/page\?page=1&page_size=10/);
  assert.match(source, /onClick=\{testNodeConnection\}/);
  assert.match(source, /disabled=\{testingNode\}/);
  assert.match(source, /正在测试…/);
  assert.match(source, /连接成功，节点与访问令牌均有效/);
  assert.match(source, /连接失败：/);
  assert.doesNotMatch(
    source,
    /测试节点连接[\s\S]{0,200}onClick=\{refreshJobs\}/,
  );
});
