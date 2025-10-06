import "@hotwired/turbo-rails";

function labelFor(state) {
  switch (state) {
    case "busy":      return "実行中...";
    case "enqueued":  return "待機中(キュー)";
    case "scheduled": return "予定済み";
    case "retry":     return "リトライ中";
    case "dead":      return "失敗(Dead)";
    case "done":      return "実行完了";
    case "idle":      return "待機中";
    default:          return "—";
  }
}

function renderState(card, state, jid) {
  const chip = card.querySelector(".status-chip");
  const btn  = card.querySelector(".run-btn");
  chip.textContent = jid ? `${labelFor(state)}${state !== "idle" ? `（JID: ジョブ起動中${jid}）` : ""}` : labelFor(state);
  chip.className = "status-chip"; // reset
  if (state === "busy" || state === "enqueued" || state === "scheduled" || state === "retry") {
    chip.classList.add("status-ok");
    btn.disabled = true;
  } else if (state === "dead") {
    chip.classList.add("status-bad");
    btn.disabled = false;
  } else if (state === "done") {
    chip.classList.add("status-ok");
    btn.disabled = false;
  } else {
    btn.disabled = false;
  }
}

async function refreshStatuses() {
  const list = document.getElementById("job-list");
  if (!list) return;
  const res = await fetch("/jobs/statuses", { headers: { "Accept": "application/json" } });
  if (!res.ok) return;
  const { statuses } = await res.json();

  [...list.querySelectorAll(".card")].forEach(card => {
    const key = card.dataset.key;
    const st  = statuses[key] || { state: "idle" };
    renderState(card, st.state, st.jid);
  });
}

function bindJobButtons() {
  const list = document.getElementById("job-list");
  if (!list) return;
  if (list.dataset.bound === "1") return;
  list.dataset.bound = "1";

  // 初回復元
  refreshStatuses();
  // 5秒ごとに更新
  setInterval(refreshStatuses, 5000);

  list.addEventListener("click", async (e) => {
    const btn = e.target.closest(".run-btn");
    if (!btn) return;

    const card = btn.closest(".card");
    const key  = btn.dataset.key;

    btn.disabled = true;
    renderState(card, "enqueued"); // 仮表示

    try {
      const res = await fetch("/jobs/run", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": document.querySelector("meta[name='csrf-token']").content
        },
        body: JSON.stringify({ key })
      });
      const data = await res.json();
      if (res.ok && (data.status || "").includes("成功")) {
        renderState(card, "busy", data.jid); // 直後は busy 表示に
      } else {
        renderState(card, "dead");
      }
    } catch {
      renderState(card, "dead");
    }
  });
}

document.addEventListener("turbo:load", bindJobButtons);
document.addEventListener("DOMContentLoaded", bindJobButtons);
console.log("[application.js] loaded with status restore");
