// kvlock status page. Renders with textContent only: nothing from /status
// is ever parsed as HTML.
"use strict";
(function () {
  var $ = function (id) { return document.getElementById(id); };
  function set(id, v, cls) {
    var el = $(id);
    el.textContent = (v === undefined || v === null || v === "") ? "-" : String(v);
    el.className = cls || "";
  }
  function dur(s) {
    if (s === undefined || s === null || s < 0) return "unknown";
    if (s < 1) return Math.round(s * 1000) + " ms";
    if (s < 120) return s.toFixed(1) + " s";
    if (s < 7200) return Math.floor(s / 60) + " min " + Math.floor(s % 60) + " s";
    return (s / 3600).toFixed(1) + " h";
  }
  function yes(b) { return b ? "yes" : "no"; }

  function renderBackup(b) {
    var card = $("backup_card");
    if (!b) { card.hidden = true; return; }
    card.hidden = false;
    var st = !b.enabled ? "disabled (observe)" : (b.running ? "running" : (b.last_error ? "failing" : (b.last_success ? "ok" : "no backup yet")));
    set("bk_status", st, b.running ? "" : (b.last_error ? "err" : (b.last_success ? "ok" : "warn")));
    var age = b.last_success ? (Date.now() - Date.parse(b.last_success)) / 1000 : -1;
    set("bk_last", b.last_success ? b.last_success + " (" + dur(age) + " ago, every " + b.interval + ")" : "never");
    set("bk_node", b.last_success_node);
    set("bk_size", b.last_size_bytes ? (b.last_size_bytes / 1048576).toFixed(1) + " MiB" : "");
    set("bk_dur", b.last_success ? dur(b.last_duration_seconds) : "");
    set("bk_key", b.last_key);
    set("bk_err", b.last_error, b.last_error ? "err" : "");
  }
  function render(s) {
    set("cluster", s.cluster);
    var badge = $("badge");
    badge.textContent = s.role + (s.mode === "observe" ? " (observe)" : "");
    badge.className = "badge " + (s.healthy ? (s.role === "leader" ? "leader" : "follower") : "bad");
    set("node", s.node); set("node_addr", s.node_addr); set("mode", s.mode);
    set("role", s.role); set("backend_role", s.backend_role); set("version", s.version);
    set("leader", s.leader || "none (vacant)", s.leader ? "" : "warn");
    set("leader_addr", s.leader_addr); set("epoch", s.epoch);
    set("leader_age", s.leader ? dur(s.leader_age_seconds) : "-");
    set("renew_age", s.last_renew_age_seconds < 0 ? "never" : dur(s.last_renew_age_seconds) + " ago",
      s.last_renew_age_seconds > 5 ? "warn" : "");
    set("master", s.master_host ? s.master_host + ":" + s.master_port : (s.backend_role === "master" ? "self (writable)" : "-"));
    set("link", s.backend_role === "slave" ? (s.master_link_up ? "up" : "DOWN") : "-",
      s.backend_role === "slave" ? (s.master_link_up ? "ok" : "err") : "");
    set("replicas", (s.replicas || []).join(", "));
    var lagCls = s.lag_seconds < 0 ? "" : (s.lag_seconds > s.max_lag_seconds ? "err" : "ok");
    set("lag_time", s.role === "leader" ? "0 (leader)" : dur(s.lag_seconds) + " (max " + dur(s.max_lag_seconds) + ")", lagCls);
    set("lag_seq", s.lag_bytes < 0 ? "unknown" : s.lag_bytes + " (max " + s.max_lag_seq + ")");
    set("gate", s.lag_gate);
    renderBackup(s.backup);
    set("healthy", yes(s.healthy), s.healthy ? "ok" : "err");
    set("kv_up", yes(s.kvrocks_up), s.kvrocks_up ? "ok" : "err");
    set("consul", s.consul_ok ? "ok" : "unreachable", s.consul_ok ? "ok" : "err");
    set("fence", s.fence_state, s.fence_pending || /WRITABLE/.test(s.fence_state || "") ? "err" : "");
    set("problems", (s.problems || []).join("; ") || "none", (s.problems || []).length ? "warn" : "ok");
    var c = s.connections || {}, parts = [];
    Object.keys(c).sort().forEach(function (k) { parts.push(k + ": " + c[k]); });
    set("conns", parts.join(", ") || "none");
    var tb = $("events");
    while (tb.firstChild) tb.removeChild(tb.firstChild);
    (s.events || []).forEach(function (e) {
      var tr = document.createElement("tr");
      [e.time ? new Date(e.time).toLocaleString() : "", e.kind, e.message].forEach(function (v) {
        var td = document.createElement("td"); td.textContent = v || ""; tr.appendChild(td);
      });
      tb.appendChild(tr);
    });
    $("admin").hidden = !s.admin_enabled;
    updateButton();
  }

  function refresh() {
    fetch("/status", { cache: "no-store" }).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (s) {
      render(s);
      set("updated", "updated " + new Date().toLocaleTimeString(), "muted");
    }).catch(function (e) {
      $("badge").textContent = "unreachable"; $("badge").className = "badge bad";
      set("updated", String(e), "muted");
    });
  }

  function updateButton() { $("handover").hidden = $("token").value.trim() === ""; }
  $("token").addEventListener("input", updateButton);
  $("handover").addEventListener("click", function () {
    if (!confirm("Hand leadership to the best replica?")) return;
    set("admin_out", "running...");
    fetch("/handover", {
      method: "POST",
      headers: { "Authorization": "Bearer " + $("token").value.trim(), "Content-Type": "application/json" },
      body: "{}"
    }).then(function (r) { return r.text().then(function (t) { return "HTTP " + r.status + "\n" + t; }); })
      .then(function (t) { set("admin_out", t); refresh(); })
      .catch(function (e) { set("admin_out", String(e)); });
  });

  refresh();
  setInterval(refresh, 2000);
})();
