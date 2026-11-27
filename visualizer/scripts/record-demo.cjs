// Records docs/images/visualizer.gif. Needs Playwright with Chromium and
// ffmpeg; run the built site first:
//
//   (cd visualizer && npm run build && npx vite preview --port 4173 &)
//   node visualizer/scripts/record-demo.cjs
//   (then the two ffmpeg commands at the end of this file's output)

const { chromium } = require('playwright');
const fs = require('fs');
(async () => {
  const out = process.env.FRAMES || 'frames';
  fs.rmSync(out, { recursive: true, force: true });
  fs.mkdirSync(out);
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1280, height: 860 }, deviceScaleFactor: 1 });
  let n = 0;
  const snap = async () => {
    await p.screenshot({ path: `${out}/f${String(n++).padStart(4, '0')}.png` });
  };
  const segment = async (fromStep, toFrameCount, every) => {
    await p.goto(`http://localhost:4173/?trace=leader-isolated.jsonl&step=${fromStep}`);
    await p.waitForSelector('svg.net');
    await p.waitForTimeout(300);
    await p.mouse.click(5, 5); // focus the page, not a control
    for (let i = 0; i < toFrameCount; i++) {
      if (i % every === 0) await snap();
      await p.keyboard.press('ArrowRight');
    }
    await snap();
  };
  await segment(502, 220, 3);
  // Hold on the two-leaders moment for a second.
  for (let i = 0; i < 8; i++) await snap();
  await segment(2701, 90, 2);
  for (let i = 0; i < 8; i++) await snap();
  console.log('frames', n);
  console.log('ffmpeg -framerate 8 -i ' + out + '/f%04d.png -vf "scale=960:-1:flags=lanczos,palettegen=max_colors=128" pal.png');
  console.log('ffmpeg -framerate 8 -i ' + out + '/f%04d.png -i pal.png -lavfi "scale=960:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=4" -loop 0 docs/images/visualizer.gif');
  await b.close();
})();
