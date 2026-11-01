// Screenshot one page of the site with the Chrome that is already on the
// machine, over the DevTools protocol, in real time.
//
// This replaced `chrome --headless=new --screenshot --virtual-time-budget`,
// which could not screenshot the team page at all. Under a virtual time
// budget Chrome pauses virtual time while a fetch is outstanding, and the
// team page's live-score poll schedules another fetch every time virtual time
// resumes, so the budget never expires and Chrome never writes the file or
// exits. It hit the team page's overview and analytics tabs at random while
// the Stats and Schedule tabs, which carry no live panel, always finished.
//
// Here the page simply loads, the script waits for a selector that only exists
// once the view has rendered, and then the screenshot is taken. No virtual
// time, no budget, and a page that fails to render fails the step by never
// producing its selector rather than by producing a picture of an error.
//
// Usage:
//   node scripts/visual_qa_screenshot.mjs --url <url> --out <file.png>
//     [--width 390] [--height 900] [--full] [--selector '.team-header']
//     [--settle 1500] [--timeout 45000] [--chrome /path/to/chrome]
import { spawn } from 'node:child_process';
import { writeFile } from 'node:fs/promises';
import process from 'node:process';

function arg(name, fallback = null) {
  const i = process.argv.indexOf('--' + name);
  if (i === -1) return fallback;
  const next = process.argv[i + 1];
  return next === undefined || next.startsWith('--') ? true : next;
}

const url = arg('url');
const out = arg('out');
if (!url || !out) {
  console.error('usage: --url <url> --out <file.png> [--width] [--height] [--full] [--selector] [--settle] [--timeout] [--chrome]');
  process.exit(2);
}
const width = Number(arg('width', 390));
const height = Number(arg('height', 900));
const full = Boolean(arg('full', false));
const selector = arg('selector', null);
const settle = Number(arg('settle', 1500));
const budget = Number(arg('timeout', 45000));
const chromePath = arg('chrome', process.env.CHROME || 'google-chrome');

const deadline = Date.now() + budget;
const left = () => deadline - Date.now();
// Unref'd, so a timer that lost a race cannot hold the process open after the
// screenshot has been written.
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms).unref());

// Chrome prints the DevTools endpoint on stderr once it is listening. Asking
// for port 0 lets the OS pick a free one, so parallel runs cannot collide.
function launch() {
  const child = spawn(chromePath, [
    '--headless=new', '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
    '--hide-scrollbars', '--force-device-scale-factor=1',
    '--window-size=' + width + ',' + height,
    '--remote-debugging-port=0', 'about:blank',
  ], { stdio: ['ignore', 'ignore', 'pipe'] });
  return new Promise((resolve, reject) => {
    let stderr = '';
    const timer = setTimeout(() => reject(new Error('chrome did not report a DevTools port')), Math.min(20000, budget));
    child.stderr.on('data', chunk => {
      stderr += chunk;
      const match = /ws:\/\/127\.0\.0\.1:(\d+)\//.exec(stderr);
      if (match) { clearTimeout(timer); resolve({ child, port: Number(match[1]) }); }
    });
    child.on('exit', code => {
      clearTimeout(timer);
      reject(new Error('chrome exited with ' + code + ': ' + stderr.slice(-400)));
    });
  });
}

class Session {
  constructor(socket) {
    this.socket = socket;
    this.id = 0;
    this.pending = new Map();
    this.events = new Map();
    socket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.id && this.pending.has(message.id)) {
        const { resolve, reject } = this.pending.get(message.id);
        this.pending.delete(message.id);
        message.error ? reject(new Error(message.error.message)) : resolve(message.result);
      } else if (message.method && this.events.has(message.method)) {
        this.events.get(message.method)();
        this.events.delete(message.method);
      }
    });
  }
  send(method, params = {}) {
    const id = ++this.id;
    this.socket.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
  }
  once(method) {
    return new Promise(resolve => this.events.set(method, resolve));
  }
}

let chrome;
try {
  const started = await launch();
  chrome = started.child;
  // Open the tab blank and navigate from there. Opening it on the URL and
  // then navigating to the same URL is a same-document navigation -- the two
  // differ only by fragment -- and fires no load event to wait for.
  const response = await fetch('http://127.0.0.1:' + started.port + '/json/new?about:blank', { method: 'PUT' });
  const target = await response.json();
  const socket = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    socket.addEventListener('open', resolve, { once: true });
    socket.addEventListener('error', () => reject(new Error('could not open a DevTools session')), { once: true });
  });
  const session = new Session(socket);
  await session.send('Page.enable');
  const loaded = session.once('Page.loadEventFired');
  await session.send('Page.navigate', { url });
  await Promise.race([loaded, sleep(Math.max(0, left()))]);

  // Everything on this site renders after its data arrives, so waiting on the
  // load event alone would photograph an empty shell. Wait for something the
  // rendered view owns.
  if (selector) {
    let seen = false;
    while (!seen && left() > 1000) {
      const probe = await session.send('Runtime.evaluate', {
        expression: '!!document.querySelector(' + JSON.stringify(selector) + ')',
        returnByValue: true,
      });
      seen = Boolean(probe.result && probe.result.value);
      if (!seen) await sleep(250);
    }
    if (!seen) throw new Error('never rendered ' + selector + ' at ' + url);
  }
  await sleep(Math.max(0, Math.min(settle, left() - 2000)));

  const shot = await session.send('Page.captureScreenshot', {
    format: 'png', captureBeyondViewport: full,
  });
  await writeFile(out, Buffer.from(shot.data, 'base64'));
  socket.close();
} catch (error) {
  console.error(String(error && error.message || error));
  process.exitCode = 1;
} finally {
  if (chrome) chrome.kill('SIGKILL');
}
