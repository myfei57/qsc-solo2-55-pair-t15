"""控制台页面。

五个页面各自只读一份状态：镜场页看辐照与对焦、吸热器页看温度带与降额、熔盐页
看罐位与联锁门限、告警页看闩锁与升级等级、任务页看清洗等长动作的进度与上一次
结果。页面不做任何写操作（任务页的续跑按钮也是调 ``/api/...`` 接口），避免
浏览器刷新就把现场设备带动一次。

长动作进度的获取方式在此定死：**轮询**，任务页每 2 秒拉一次
``GET /api/jobs``，不使用任何推送通道。
"""

from __future__ import annotations

from html import escape

from ..app import Platform

PAGE_PATHS: tuple[str, ...] = ("/", "/receiver", "/salt", "/alarms", "/jobs")

_POLL_INTERVAL_MS = 2000

_STYLE = """
body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 0; background: #0f1720; color: #e6edf3; }
header { padding: 14px 20px; background: #16202b; border-bottom: 1px solid #223041; }
nav a { color: #8ecdf7; margin-right: 14px; text-decoration: none; }
main { padding: 18px 20px 40px; }
table { border-collapse: collapse; width: 100%; margin-bottom: 18px; }
th, td { border: 1px solid #223041; padding: 6px 10px; text-align: left; font-size: 14px; }
th { background: #17222e; }
.alarm { color: #ff8080; font-weight: 600; }
.ok { color: #7ee787; }
.warn { color: #f0c674; }
.bar { background: #223041; border-radius: 3px; height: 14px; width: 160px; overflow: hidden; }
.bar > i { display: block; height: 14px; background: #388bfd; }
button { background: #26404f; color: #e6edf3; border: 1px solid #34556b; border-radius: 4px; padding: 4px 10px; cursor: pointer; }
"""


def _table(title: str, rows: list[tuple[str, object]]) -> str:
    """渲染一张两列数据表。"""

    body = "".join(
        f"<tr><th>{escape(str(key))}</th><td>{escape(str(value))}</td></tr>" for key, value in rows
    )
    return f"<h2>{escape(title)}</h2><table>{body}</table>"


def _shell(title: str, body: str) -> str:
    """套上页头与导航。"""

    nav = "".join(
        f'<a href="{path}">{label}</a>'
        for path, label in (
            ("/", "镜场"),
            ("/receiver", "吸热器"),
            ("/salt", "熔盐"),
            ("/alarms", "告警"),
            ("/jobs", "任务"),
        )
    )
    return (
        "<!doctype html><html lang=\"zh\"><head><meta charset=\"utf-8\">"
        f"<title>{escape(title)}</title><style>{_STYLE}</style></head>"
        f"<body><header><strong>{escape(title)}</strong><nav style=\"margin-top:8px\">{nav}</nav>"
        f"</header><main>{body}</main></body></html>"
    )


def _mirror_page(platform: Platform) -> str:
    """镜场页：辐照、策略、对焦与调度。"""

    status = platform.status()["field"]
    rows = [
        ("运行模式", status["mode"]),
        ("判定依据", status["reason"]),
        ("当前辐照 (W/m²)", status["dni_w_m2"]),
        ("已落盘辐照 (W/m²)", status["durable_dni_w_m2"]),
        ("采样 / 拒绝 / 落盘次数", f"{status['samples']} / {status['rejected']} / {status['persists']}"),
        ("对焦镜数", f"{status['focusing']} / {status['mirrors']['count']}"),
        ("避让镜数", status["excluded"]),
        ("手动接管镜数", status["manual"]),
        ("执行负荷 (%)", status["dispatch_pct"]),
        ("调度目标 (%)", status["dispatch_target_pct"]),
        ("避让区挡下次数", status["zone_vetoes"]),
        ("清洗窗口数", status["windows"]),
        ("热功率 (kW)", round(platform.field.thermal_power_kw(), 1)),
    ]
    return _shell("镜场控制台", _table("镜场状态", rows))


def _receiver_page(platform: Platform) -> str:
    """吸热器页：温度带、流量门控与降额。"""

    snapshot = platform.status()["receiver"]
    report = platform.temperature_report()
    rows = [
        ("状态", snapshot["state"]),
        ("出口温度 (℃)", snapshot["temperature_c"]),
        ("峰值 (℃)", snapshot["peak_c"]),
        ("目标 / 降额 / 上限 (℃)", f"{report['target_c']} / {report['derate_c']} / {report['max_c']}"),
        ("距降额余量 (℃)", report["margin_to_derate_c"]),
        ("距上限余量 (℃)", report["margin_to_max_c"]),
        ("是否降额", "是" if snapshot["derated"] else "否"),
        ("出力上限 (%)", snapshot["ceiling_pct"]),
        ("进口阀开度 (%)", snapshot["valve_pct"]),
        ("流量 (kg/s)", snapshot["flow_kg_s"]),
        ("流量确认", "已确认" if snapshot["flow_confirmed"] else "未确认"),
        ("确认累计 (s)", snapshot["hold_s"]),
        ("跳闸原因", snapshot["trip_reason"] or "-"),
    ]
    return _shell("吸热器控制台", _table("吸热器状态", rows))


def _salt_page(platform: Platform) -> str:
    """熔盐页：罐位、泵与联锁门限。"""

    salt = platform.status()["salt"]
    rows = [
        ("冷盐罐液位 (%)", f"{salt['cold_tank']['level_pct']} ({salt['cold_tank']['level_kg']} kg)"),
        ("热盐罐液位 (%)", f"{salt['hot_tank']['level_pct']} ({salt['hot_tank']['level_kg']} kg)"),
        ("冷泵流量 (kg/s)", salt["cold_pump"]["flow_kg_s"]),
        ("热泵流量 (kg/s)", salt["hot_pump"]["flow_kg_s"]),
        ("母管净流量 (kg/s)", salt["net_flow_kg_s"]),
        ("母管压力 (MPa)", salt["header"]["pressure_mpa"]),
        ("在运泵数", salt["cold_pump"]["running"] + salt["hot_pump"]["running"]),
        ("两泵对顶步数", salt["opposed_steps"]),
        ("流量基线 (kg/s)", salt["baseline_kg_s"]),
        ("联锁门限 (kg/s)", salt["threshold_kg_s"]),
        ("标定代际", salt["epoch"]),
        ("成功切换次数", salt["changeovers"]),
    ]
    return _shell("熔盐控制台", _table("熔盐回路", rows))


def _alarm_page(platform: Platform) -> str:
    """告警页：闩锁、历史与升级等级。"""

    status = platform.status()
    alarm = status["alarm"]
    rows = [
        ("闩锁状态", "已置位" if alarm["latched"] else "已复位"),
        ("跳闸来源", alarm["source"] or "-"),
        ("跳闸原因", alarm["reason"] or "-"),
        ("累计置位", alarm["trips"]),
        ("告警条数", alarm["history"]),
        ("按规则计数", alarm["by_rule"] or "-"),
    ]
    body = _table("闩锁与统计", rows)
    level_rows = [(item["rule"], f"{item['name']}（{item['occurrences']} 次）") for item in status["levels"]]
    body += _table("升级等级", level_rows)
    recent = platform.history.recent(12)
    recent_rows = [
        (f"{entry['ts']}", f"{entry['rule']} {entry['message']} = {entry['value']}") for entry in recent
    ] or [("暂无", "-")]
    body += _table("最近告警", recent_rows)
    body += _table("规则表", [(item["name"], f"{item['signal']} {'≥' if item['above'] else '≤'} {item['threshold']}") for item in platform.rules.describe()])
    return _shell("告警控制台", body)


def _jobs_page(platform: Platform) -> str:
    """任务页：长动作（清洗）的实时进度与最近一次结果，定死 2 秒轮询。"""

    initial = platform.commands.list_jobs(limit=20)
    rows = "".join(_job_row(job) for job in initial) or (
        '<tr><td colspan="8">暂无任务；清洗动作提交后会出现在这里</td></tr>'
    )
    script = """
<script>
function esc(v) { return String(v === null || v === undefined ? '' : v); }
function jobRow(j) {
  var pct = Math.round((j.progress || 0) * 100);
  var cls = j.state === 'succeeded' ? 'ok' : (j.state === 'interrupted' || j.state === 'failed' || j.state === 'rejected' ? 'alarm' : 'warn');
  var resume = j.state === 'interrupted'
    ? '<button onclick="resumeJob(\\'' + j.job_id + '\\')">续跑剩余镜子</button>' : '';
  var detail = esc(j.stage) + '：' + esc(j.message)
    + (j.violations && j.violations.length ? '（' + esc(j.violations.join(', ')) + '）' : '');
  return '<tr><td>' + esc(j.job_id) + '</td><td>' + esc(j.action) + '</td>'
    + '<td class="' + cls + '">' + esc(j.state) + '</td>'
    + '<td>' + (j.state === 'queued' ? '排队#' + esc(j.queue_position) : esc(j.current_mirror || '-')) + '</td>'
    + '<td><div class="bar"><i style="width:' + pct + '%"></i></div>' + pct + '% ('
    + esc(j.succeeded) + '/' + esc(j.total) + ' 成功' + (j.failed ? '，' + esc(j.failed) + ' 失败' : '') + '</td>'
    + '<td>' + detail + '</td><td>' + esc(j.request_id) + '</td><td>' + resume + '</td></tr>';
}
async function refreshJobs() {
  try {
    var resp = await fetch('/api/jobs?limit=20');
    var data = await resp.json();
    document.querySelector('#jobs tbody').innerHTML =
      data.jobs.length ? data.jobs.map(jobRow).join('')
      : '<tr><td colspan="8">暂无任务</td></tr>';
  } catch (e) { /* 网络抖动时保留上一次画面，下个周期再试 */ }
}
async function resumeJob(jobId) {
  var requestId = 'req-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
  var resp = await fetch('/api/wash/resume', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({job_id: jobId, request_id: requestId})
  });
  await resp.json();
  refreshJobs();
}
setInterval(refreshJobs, __POLL_MS__);
</script>
""".replace("__POLL_MS__", str(_POLL_INTERVAL_MS))
    body = (
        '<h2>长动作任务（清洗）</h2>'
        '<p>进度按固定 2 秒轮询刷新；中断的任务标记为部分失败，可用"续跑"只重洗未完成的镜子。</p>'
        f'<table id="jobs"><thead><tr><th>任务</th><th>动作</th><th>状态</th><th>当前/排队</th>'
        f'<th>进度</th><th>卡在哪一步 / 结果</th><th>提交标识</th><th></th></tr></thead>'
        f'<tbody>{rows}</tbody></table>{script}'
    )
    return _shell("任务控制台", body)


def _job_row(job: dict) -> str:
    """服务端首屏渲染一行任务（与前端 JS 的字段口径一致）。"""

    pct = round(float(job.get("progress", 0.0)) * 100)
    state = str(job.get("state", ""))
    cls = "ok" if state == "succeeded" else ("alarm" if state in ("interrupted", "failed", "rejected") else "warn")
    if state == "queued":
        here = f"排队#{job.get('queue_position', 0)}"
    else:
        here = escape(str(job.get("current_mirror") or "-"))
    violations = job.get("violations") or []
    detail = escape(f"{job.get('stage', '')}：{job.get('message', '')}")
    if violations:
        detail += f"（{escape(', '.join(str(v) for v in violations))}）"
    resume = ""
    if state == "interrupted":
        resume = (
            f'<button onclick="resumeJob(\'{escape(str(job.get("job_id", "")))}\')">'
            "续跑剩余镜子</button>"
        )
    return (
        "<tr>"
        f'<td>{escape(str(job.get("job_id", "")))}</td>'
        f'<td>{escape(str(job.get("action", "")))}</td>'
        f'<td class="{cls}">{escape(state)}</td>'
        f"<td>{here}</td>"
        f'<td><div class="bar"><i style="width:{pct}%"></i></div>{pct}% '
        f'({job.get("succeeded", 0)}/{job.get("total", 0)} 成功'
        + (f"，{job.get('failed', 0)} 失败" if job.get("failed") else "")
        + ")</td>"
        f"<td>{detail}</td>"
        f'<td>{escape(str(job.get("request_id", "")))}</td>'
        f"<td>{resume}</td>"
        "</tr>"
    )


_RENDERERS = {
    "/": _mirror_page,
    "/receiver": _receiver_page,
    "/salt": _salt_page,
    "/alarms": _alarm_page,
    "/jobs": _jobs_page,
}


def render_page(path: str, platform: Platform) -> str | None:
    """渲染一个页面；路径不是页面时返回 ``None``。"""

    renderer = _RENDERERS.get(path)
    if renderer is None:
        return None
    return renderer(platform)
