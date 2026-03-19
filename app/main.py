# Запуск:
#   Часть 1 — client/server:
#     docker compose up --build
#
#   Часть 2 — producer/consumer + дашборд:
#     docker compose -f docker-compose.advanced.yml up --build --scale consumer=3
#
#   Часть 3 — эксперимент масштабирования:
#     ./run_benchmark.sh
#
# Локальная сеть:
#   Дашборд:            http://localhost:8080
#   RabbitMQ UI:        http://localhost:15672  (guest / guest)
#   HTTP сервер (ч.1):  http://localhost:5001

import os
import sys
import time
import json
import random
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

APP_MODE          = os.environ.get("APP_MODE", "server")
SERVER_HOST       = os.environ.get("SERVER_HOST", "server")
SERVER_PORT       = int(os.environ.get("SERVER_PORT", "5000"))
RABBITMQ_HOST     = os.environ.get("RABBITMQ_HOST", "rabbitmq")
WORKER_ID         = os.environ.get("WORKER_ID", "1")
QUEUE_NAME        = "tasks"

TASK_COUNT        = int(os.environ.get("TASK_COUNT",          "16"))
IMG_WIDTH         = int(os.environ.get("IMG_WIDTH",           "800"))
IMG_HEIGHT        = int(os.environ.get("IMG_HEIGHT",          "800"))
BLUR_PASSES       = int(os.environ.get("BLUR_PASSES",         "12"))
CPU_BURN_N        = int(os.environ.get("CPU_BURN_ITERATIONS",  "20000000"))
PRODUCER_INTERVAL = float(os.environ.get("PRODUCER_INTERVAL", "2.0"))


def wait_for_rabbitmq(host: str, retries: int = 20, delay: int = 3):
    import pika
    for attempt in range(1, retries + 1):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            conn.close()
            log.info("RabbitMQ ready")
            return
        except Exception:
            log.warning("RabbitMQ not ready (%d/%d), retry in %ds…", attempt, retries, delay)
            time.sleep(delay)
    log.error("Could not connect to RabbitMQ after %d attempts", retries)
    sys.exit(1)


def process_image(width: int, height: int, blur_passes: int, seed: int, burn_n: int) -> int:
    import numpy as np
    from PIL import Image, ImageFilter

    rng = np.random.RandomState(seed)
    data = rng.randint(0, 256, (height, width, 3), dtype=np.uint8)
    img = Image.fromarray(data, "RGB")
    for _ in range(blur_passes):
        img = img.filter(ImageFilter.GaussianBlur(radius=5))
    checksum = int(np.array(img).sum())

    acc = 0.0
    for i in range(1, burn_n + 1):
        acc += i / (i + 1.0)
    checksum ^= int(acc) & 0xFFFFFFFF

    return checksum


def run_server():
    from flask import Flask, request, jsonify

    app = Flask(__name__)

    @app.route("/ping", methods=["GET"])
    def ping():
        return jsonify({"status": "ok", "mode": "server"})

    @app.route("/compute", methods=["POST"])
    def compute():
        data = request.get_json(force=True)
        operation = data.get("op", "add")
        a, b = float(data.get("a", 0)), float(data.get("b", 0))

        if operation == "add":
            result = a + b
        elif operation == "mul":
            result = a * b
        elif operation == "pow":
            result = a ** b
        else:
            return jsonify({"error": f"unknown operation: {operation}"}), 400

        log.info("compute %s(%s, %s) = %s", operation, a, b, result)
        return jsonify({"op": operation, "a": a, "b": b, "result": result})

    log.info("Server starting on 0.0.0.0:%d", SERVER_PORT)
    app.run(host="0.0.0.0", port=SERVER_PORT)


def run_client():
    import requests

    url = f"http://{SERVER_HOST}:{SERVER_PORT}"
    ops = ["add", "mul", "pow"]

    log.info("Client waiting for server at %s …", url)
    for _ in range(20):
        try:
            requests.get(f"{url}/ping", timeout=2)
            log.info("Server is up!")
            break
        except Exception:
            time.sleep(1)
    else:
        log.error("Server unreachable, giving up")
        sys.exit(1)

    while True:
        a = random.randint(1, 10)
        b = random.randint(1, 5)
        op = random.choice(ops)
        try:
            resp = requests.post(f"{url}/compute", json={"op": op, "a": a, "b": b}, timeout=5)
            data = resp.json()
            log.info("→ %s(%s, %s) = %s", op, a, b, data.get("result"))
        except Exception as exc:
            log.error("Request failed: %s", exc)
        time.sleep(2)


def run_consumer():
    import pika

    wait_for_rabbitmq(RABBITMQ_HOST)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_task(ch, method, props, body):
        task = json.loads(body)
        tid  = task["task_id"]
        w, h = task["width"], task["height"]
        bp   = task["blur_passes"]
        seed = task["seed"]

        log.info("[worker-%s] task #%d  %dx%d  %d passes — started", WORKER_ID, tid, w, h, bp)
        t0 = time.time()
        checksum = process_image(w, h, bp, seed, CPU_BURN_N)
        elapsed  = time.time() - t0
        log.info("[worker-%s] task #%d  done in %.2fs  checksum=%d", WORKER_ID, tid, elapsed, checksum)

        if props.reply_to:
            result = {
                "task_id":   tid,
                "worker_id": WORKER_ID,
                "elapsed_s": round(elapsed, 3),
                "checksum":  checksum,
            }
            ch.basic_publish(
                exchange="",
                routing_key=props.reply_to,
                body=json.dumps(result),
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id,
                ),
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_task)
    log.info("Worker-%s listening on queue '%s'…", WORKER_ID, QUEUE_NAME)
    channel.start_consuming()


def run_producer():
    import pika

    wait_for_rabbitmq(RABBITMQ_HOST)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    log.info("Producer started → queue '%s'", QUEUE_NAME)
    task_id = 1

    while True:
        task = {
            "task_id":     task_id,
            "width":       IMG_WIDTH,
            "height":      IMG_HEIGHT,
            "blur_passes": BLUR_PASSES,
            "seed":        task_id * 17,
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(task),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        log.info("Published task #%d  %dx%d  %d passes", task_id, IMG_WIDTH, IMG_HEIGHT, BLUR_PASSES)
        task_id += 1
        time.sleep(PRODUCER_INTERVAL)


def run_benchmark():
    import pika

    wait_for_rabbitmq(RABBITMQ_HOST)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    reply_q = channel.queue_declare(queue="", exclusive=True).method.queue
    results: dict = {}

    def on_result(ch, method, props, body):
        data = json.loads(body)
        results[data["task_id"]] = data
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=reply_q, on_message_callback=on_result)

    log.info("Waiting 4s for consumers to be ready…")
    time.sleep(4)

    log.info(
        "Benchmark start: %d tasks  |  %dx%d image  |  %d blur passes/task",
        TASK_COUNT, IMG_WIDTH, IMG_HEIGHT, BLUR_PASSES,
    )

    t_start = time.time()

    for i in range(1, TASK_COUNT + 1):
        task = {
            "task_id":     i,
            "width":       IMG_WIDTH,
            "height":      IMG_HEIGHT,
            "blur_passes": BLUR_PASSES,
            "seed":        i * 42,
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(task),
            properties=pika.BasicProperties(
                delivery_mode=2,
                reply_to=reply_q,
                correlation_id=str(i),
            ),
        )

    log.info("All %d tasks sent, waiting for results…", TASK_COUNT)

    while len(results) < TASK_COUNT:
        connection.process_data_events(time_limit=1)

    elapsed    = time.time() - t_start
    throughput = TASK_COUNT / elapsed
    avg        = elapsed / TASK_COUNT
    worker_ids = sorted({r["worker_id"] for r in results.values()})

    log.info("")
    log.info("╔══════════════════════════════════════════╗")
    log.info("║           BENCHMARK  RESULTS             ║")
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  Tasks        : %-26d║", TASK_COUNT)
    log.info("║  Image size   : %dx%d%-*s║", IMG_WIDTH, IMG_HEIGHT, 18 - len(f"{IMG_WIDTH}x{IMG_HEIGHT}"), "")
    log.info("║  Blur passes  : %-26d║", BLUR_PASSES)
    log.info("║  Workers used : %-26s║", ", ".join(map(str, worker_ids)))
    log.info("║  Total time   : %-24.2fs ║", elapsed)
    log.info("║  Throughput   : %-19.2f tasks/s ║", throughput)
    log.info("║  Avg per task : %-24.2fs ║", avg)
    log.info("╚══════════════════════════════════════════╝")
    log.info("")

    connection.close()


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>Task Queue Monitor</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Courier New', monospace; background: #0d1117; color: #c9d1d9;
         padding: 24px; min-height: 100vh; }
  h1 { color: #58a6ff; font-size: 1.4em; margin-bottom: 20px; letter-spacing: 1px; }
  .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 18px; }
  .card .val { font-size: 2.6em; font-weight: bold; color: #58a6ff; line-height: 1.1; }
  .card .lbl { color: #8b949e; font-size: 0.75em; margin-top: 4px; text-transform: uppercase; }
  .card.green .val { color: #3fb950; }
  .card.yellow .val { color: #d29922; }
  .card.red .val { color: #f85149; }
  .bar-wrap { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
              padding: 18px; margin-bottom: 16px; }
  .bar-wrap h2 { color: #8b949e; font-size: 0.8em; margin-bottom: 10px; text-transform: uppercase; }
  .bar-track { background: #21262d; border-radius: 4px; height: 26px; overflow: hidden; }
  .bar-fill  { height: 100%; background: linear-gradient(90deg, #1f6feb, #58a6ff);
               transition: width 0.6s ease; display: flex; align-items: center;
               padding-left: 8px; font-size: 0.8em; font-weight: bold; min-width: 2px; }
  .log-wrap { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 18px; }
  .log-wrap h2 { color: #8b949e; font-size: 0.8em; margin-bottom: 10px; text-transform: uppercase; }
  .log-row { padding: 5px 8px; font-size: 0.82em; border-left: 3px solid #30363d;
             margin-bottom: 4px; background: #0d1117; border-radius: 0 4px 4px 0;
             animation: fadein 0.4s; }
  .log-row.pub  { border-color: #1f6feb; }
  .log-row.done { border-color: #3fb950; }
  @keyframes fadein { from { opacity: 0; transform: translateX(-6px); } to { opacity: 1; } }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
         background: #3fb950; margin-right: 6px; animation: pulse 1.2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
</style>
</head>
<body>
<h1><span class="dot"></span>Task Queue Monitor &mdash; RabbitMQ + Docker</h1>
<div class="grid">
  <div class="card yellow">
    <div class="val" id="ready">—</div>
    <div class="lbl">В очереди (ready)</div>
  </div>
  <div class="card">
    <div class="val" id="unacked">—</div>
    <div class="lbl">Обрабатывается</div>
  </div>
  <div class="card green">
    <div class="val" id="delivered">—</div>
    <div class="lbl">Выполнено (всего)</div>
  </div>
  <div class="card">
    <div class="val" id="rate">—</div>
    <div class="lbl">tasks / sec</div>
  </div>
</div>
<div class="bar-wrap">
  <h2>Загрузка очереди</h2>
  <div style="margin-bottom:6px;font-size:.8em;color:#8b949e" id="bar-label"></div>
  <div class="bar-track">
    <div class="bar-fill" id="bar" style="width:0%"></div>
  </div>
</div>
<div class="bar-wrap">
  <h2>Скорость обработки (last 60 s)</h2>
  <canvas id="chart" width="0" height="60" style="width:100%;height:60px;display:block;"></canvas>
</div>
<div class="log-wrap">
  <h2>Лог событий (авто-обновление 1 с)</h2>
  <div id="log"></div>
</div>
<script>
const history = [];
let lastDelivered = null;
function ts() { return new Date().toLocaleTimeString('ru-RU'); }
function addLog(msg, cls) {
  const log = document.getElementById('log');
  const row = document.createElement('div');
  row.className = 'log-row ' + (cls||'');
  row.textContent = ts() + '  ' + msg;
  log.prepend(row);
  while (log.children.length > 14) log.removeChild(log.lastChild);
}
async function update() {
  let data;
  try {
    const r = await fetch('/api/stats');
    data = await r.json();
  } catch(e) {
    addLog('⚠ Нет связи с RabbitMQ...', '');
    return;
  }
  const ready     = data.messages_ready          ?? 0;
  const unacked   = data.messages_unacknowledged ?? 0;
  const stats     = data.message_stats           ?? {};
  const delivered = stats.deliver_get            ?? 0;
  const published = stats.publish                ?? 0;
  const rateRaw   = stats.deliver_get_details?.rate ?? 0;
  const rate      = rateRaw.toFixed(2);
  document.getElementById('ready').textContent     = ready;
  document.getElementById('unacked').textContent   = unacked;
  document.getElementById('delivered').textContent = delivered;
  document.getElementById('rate').textContent      = rate;
  const pct = Math.min(100, ((ready / (ready + unacked + 1)) * 100)).toFixed(0);
  document.getElementById('bar').style.width = pct + '%';
  document.getElementById('bar').textContent = ready > 0 ? ready + ' задач ждут' : '';
  document.getElementById('bar-label').textContent =
    `published: ${published}  |  delivered: ${delivered}  |  in-flight: ${unacked}`;
  if (lastDelivered !== null && delivered > lastDelivered) {
    addLog(`✓ Выполнено +${delivered - lastDelivered}  (итого ${delivered})`, 'done');
  }
  if (ready > 0 && (lastDelivered === null || delivered === lastDelivered)) {
    addLog(`⏳ В очереди ${ready} задач, обрабатывается ${unacked}`, 'pub');
  }
  lastDelivered = delivered;
  history.push(parseFloat(rate));
  if (history.length > 60) history.shift();
  drawChart();
}
function drawChart() {
  const canvas = document.getElementById('chart');
  canvas.width = canvas.offsetWidth;
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  if (history.length < 2) return;
  const mx = Math.max(...history, 0.1);
  ctx.strokeStyle = '#58a6ff';
  ctx.lineWidth = 2;
  ctx.beginPath();
  history.forEach((v, i) => {
    const x = (i / (history.length - 1)) * W;
    const y = H - (v / mx) * (H - 4) - 2;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.lineTo(W, H); ctx.lineTo(0, H); ctx.closePath();
  ctx.fillStyle = 'rgba(88,166,255,0.08)';
  ctx.fill();
}
update();
setInterval(update, 1000);
</script>
</body>
</html>"""


def run_dashboard():
    import requests as req
    from flask import Flask, jsonify

    RABBIT_API = f"http://{RABBITMQ_HOST}:15672/api"
    app = Flask(__name__)

    @app.route("/")
    def index():
        return _DASHBOARD_HTML

    @app.route("/api/stats")
    def stats():
        try:
            r = req.get(
                f"{RABBIT_API}/queues/%2F/{QUEUE_NAME}",
                auth=("guest", "guest"),
                timeout=3,
            )
            return jsonify(r.json())
        except Exception as exc:
            return jsonify({"error": str(exc)}), 503

    log.info("Dashboard → http://localhost:8080")
    app.run(host="0.0.0.0", port=8080)


MODES = {
    "server":    run_server,
    "client":    run_client,
    "producer":  run_producer,
    "consumer":  run_consumer,
    "benchmark": run_benchmark,
    "dashboard": run_dashboard,
}

if APP_MODE not in MODES:
    log.error("Unknown APP_MODE=%r. Choose from: %s", APP_MODE, ", ".join(MODES))
    sys.exit(1)

log.info("Starting in mode: %s", APP_MODE)
MODES[APP_MODE]()
