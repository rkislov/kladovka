const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const folderIcon = '<svg viewBox="0 0 24 24" class="size-5" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M3 7a2 2 0 0 1 2-2h5l2 3h7a2 2 0 0 1 2 2v9H3z"/></svg>';
let timer, openPolka = null, busy = false, refreshing = false, polki = [], signature = '', limits = {};

function objectCount(n) {
  const mod = n % 100;
  return n + ' ' + (mod >= 11 && mod <= 14 ? 'объектов' : n % 10 === 1 ? 'объект' : n % 10 >= 2 && n % 10 <= 4 ? 'объекта' : 'объектов');
}
function bytes(n) {
  n = Number(n) || 0;
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < 4) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + units[i];
}
function uptime(n) {
  n = Math.floor(n || 0);
  return (n >= 3600 ? Math.floor(n / 3600) + ' ч ' : '') + Math.floor(n % 3600 / 60) + ' мин';
}
function toast(message, error = false) {
  $('toast').textContent = message;
  $('toast').className = 'toast show' + (error ? ' err' : '');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => $('toast').classList.remove('show'), 6000);
}
async function api(url, options = {}) {
  const response = await fetch(new URL(url, window.location.origin), { credentials: 'same-origin', ...options });
  if (!response.ok) {
    const body = await response.text();
    const messages = { 401: 'Нужен вход: обновите страницу и введите реквизиты.', 413: 'Файл превышает допустимый размер.', 507: 'Не хватает места или превышена квота хранилища.', 503: 'Хранилище временно недоступно. Повторите действие позже.' };
    const xml = new DOMParser().parseFromString(body, 'application/xml');
    throw Error(messages[response.status] || xml.querySelector('Message')?.textContent || body.slice(0, 300) || 'Ошибка ' + response.status);
  }
  return response;
}
function objectURL(name, key) {
  return '/api/object/' + encodeURIComponent(name) + '/' + key.split('/').map(encodeURIComponent).join('/');
}
function renderStats(j) {
  const m = j.metrics || {};
  limits = j.limits || {};
  const quota = j.quota || {};
  $('quota-status').hidden = !quota.total_bytes;
  $('quota-status').textContent = `Квота данных: ${bytes(quota.used_bytes)} из ${bytes(quota.total_bytes)}`;
  const cards = [
    ['Полки', j.polki_count, 'Для ваших файлов', 'sec-polki'],
    ['Объекты', j.objects_count, 'В локальном хранилище', 'sec-polki'],
    ['Объём файлов', bytes(j.objects_bytes), 'Без служебных данных', 'sec-polki'],
    ['Доступные узлы', j.nodes_healthy + ' / ' + j.nodes_total, 'Копий объекта: ' + j.replication_factor, 'sec-nodes'],
  ];
  $('stats').innerHTML = cards.map(([label, value, hint, go]) => `<button class="stat" data-go="${go}"><span class="label">${esc(label)}</span><span class="value">${esc(value)}</span><span class="hint">${esc(hint)}</span></button>`).join('');
  $('stats').querySelectorAll('button').forEach(b => b.onclick = () => $(b.dataset.go).scrollIntoView({ behavior: 'smooth' }));
  const total = m.storage_total_bytes || 0, used = m.storage_used_bytes || 0;
  const pct = total ? Math.min(100, Math.round(100 * used / total)) : 0;
  $('diskbar').style.width = pct + '%';
  $('diskbar').classList.toggle('bg-amber-500', pct >= 85);
  $('diskbar').classList.toggle('bg-blue-500', pct < 85);
  $('disk-meter').setAttribute('aria-valuenow', String(pct));
  $('disk-percent').textContent = total ? pct + '%' : '—';
  $('disklabel').textContent = total ? `${bytes(used)} из ${bytes(total)} · свободно ${bytes(Math.max(0, total - used))}` : 'Ёмкость файловой системы неизвестна';
  $('activity').innerHTML = [ ['Загружено', bytes(m.bytes_uploaded_total)], ['Скачано', bytes(m.bytes_downloaded_total)], ['S3-операции', m.s3_operations_total], ['HTTP-запросы', m.http_requests_total] ].map(([name, value]) => `<div><p class="text-xs text-slate-500">${esc(name)}</p><p class="mt-2 text-lg font-semibold text-slate-800">${esc(value)}</p></div>`).join('');
  $('uptime').textContent = uptime(m.uptime_seconds);
  $('ops').innerHTML = Object.entries(m.s3_by_operation || {}).map(([k, v]) => `<span class="chip">${esc(k)} <b>${esc(v)}</b></span>`).join('') + `<span class="chip">Репликация: успешно ${esc(m.replication_ok_total || 0)}, ошибок ${esc(m.replication_failed_total || 0)}</span>`;
  $('updated').textContent = 'Обновлено ' + new Date().toLocaleTimeString();
}
function renderPolki(force = false) {
  const filter = $('filter').value.toLowerCase(), next = JSON.stringify([polki, filter, openPolka]);
  if (!force && next === signature) return;
  signature = next;
  $('polki-count').textContent = String(polki.length);
  const visible = polki.filter(p => p.name.toLowerCase().includes(filter));
  $('polki').innerHTML = visible.map(p => `<article><button class="polka" data-name="${esc(p.name)}" aria-expanded="${p.name === openPolka}"><span class="polka-icon">${folderIcon}</span><span class="min-w-0 flex-1"><strong class="block truncate text-sm font-semibold text-slate-800">${esc(p.name)}</strong><span class="mt-1 block text-xs text-slate-500">${esc(objectCount(p.objects))} · ${bytes(p.bytes)}</span></span><span class="text-slate-500" aria-hidden="true">${p.name === openPolka ? '−' : '+'}</span></button><div class="detail" ${p.name === openPolka ? '' : 'hidden'}></div></article>`).join('') || `<p class="empty">${polki.length ? 'Нет полок с таким названием.' : 'Здесь появятся ваши файлы.<br>Создайте первую полку, чтобы начать.'}</p>`;
  $('polki').querySelectorAll('.polka').forEach(button => button.onclick = () => {
    openPolka = openPolka === button.dataset.name ? null : button.dataset.name;
    renderPolki(true);
  });
  const button = [...$('polki').querySelectorAll('.polka')].find(b => b.dataset.name === openPolka);
  if (button) openDetail(button.parentElement.querySelector('.detail'), openPolka);
}
async function openDetail(detail, name) {
  detail.innerHTML = `<div class="space-y-4 p-5"><div class="flex flex-wrap items-center justify-between gap-3"><label class="flex min-w-0 flex-1 items-center gap-2 text-xs text-slate-500">Папка<input class="input prefix min-w-0 flex-1" placeholder="Например, photos/" aria-label="Папка для загрузки" /></label><button class="danger delete-polka" data-mutation>Удалить полку</button></div><label class="dropzone"><svg class="size-6 text-blue-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M12 16V3m-5 5 5-5 5 5M4 14v6h16v-6"/></svg><span><span class="font-medium text-blue-600">Выберите файлы</span> или перетащите сюда</span><span class="text-xs text-slate-500">Файл с таким же именем будет заменён</span><input type="file" multiple class="upload sr-only" aria-label="Загрузить файлы" data-mutation /></label><p class="upload-state meta" role="status" hidden></p></div><ul class="objs divide-y divide-slate-100 border-t border-slate-100"></ul><div class="flex items-center justify-between gap-3 px-5 py-3"><p class="meta list-state">Получаем список файлов…</p><button class="ghost more" hidden>Показать ещё</button></div>`;
  detail.querySelector('.delete-polka').onclick = async () => {
    if (!confirm('Удалить пустую полку «' + name + '»?')) return;
    await action(async () => { await api('/api/polki/' + encodeURIComponent(name), { method: 'DELETE' }); openPolka = null; signature = ''; await refresh(); toast('Полка удалена'); });
  };
  const upload = async files => {
    if (!files.length || busy) return;
    const effectiveLimit = Math.min(...[limits.max_object_size, limits.max_temp_bytes].filter(n => n > 0));
    if (files.some(file => file.size > effectiveLimit)) { toast('Размер одного файла не должен превышать ' + bytes(effectiveLimit), true); return; }
    let prefix = detail.querySelector('.prefix').value.replace(/^\/+|\/+$/g, '');
    if (prefix) prefix += '/';
    await action(async () => {
      const status = detail.querySelector('.upload-state');
      status.hidden = false;
      for (let i = 0; i < files.length; i++) {
        status.textContent = `Загружаем ${i + 1} из ${files.length}: ${files[i].name}`;
        await api(objectURL(name, prefix + files[i].name), { method: 'PUT', headers: { 'Content-Type': files[i].type || 'application/octet-stream' }, body: files[i] });
      }
      signature = ''; await refresh(); toast('Файлы загружены: ' + files.length);
    });
    if (detail.isConnected) { detail.querySelector('.upload-state').hidden = true; detail.querySelector('.upload').value = ''; }
  };
  detail.querySelector('.upload').onchange = e => upload([...e.target.files]);
  const zone = detail.querySelector('.dropzone');
  zone.ondragover = e => { e.preventDefault(); zone.dataset.dragging = 'true'; };
  zone.ondragleave = () => { delete zone.dataset.dragging; };
  zone.ondrop = e => { e.preventDefault(); delete zone.dataset.dragging; upload([...e.dataTransfer.files]); };
  let after = '';
  async function page() {
    const button = detail.querySelector('.more');
    button.disabled = true;
    try {
      const j = await (await api('/api/polki/' + encodeURIComponent(name) + '?start-after=' + encodeURIComponent(after))).json();
      const list = detail.querySelector('.objs');
      for (const object of j.objects || []) {
        const li = document.createElement('li');
        li.className = 'flex flex-wrap items-center gap-3 px-5 py-3.5';
        li.innerHTML = `<svg class="size-5 shrink-0 text-slate-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true"><path d="M5 3h9l5 5v13H5zM14 3v6h5"/></svg><div class="min-w-0 flex-1"><span class="block break-all text-sm font-medium text-slate-700">${esc(object.key.split('/').pop())}</span><span class="mt-0.5 block break-all text-xs text-slate-500">${object.key.includes('/') ? esc(object.key.slice(0, object.key.lastIndexOf('/') + 1)) + ' · ' : ''}${bytes(object.size)}${object.last_modified ? ' · ' + esc(new Date(object.last_modified).toLocaleDateString()) : ''}</span></div><a class="rounded-md px-2 py-1.5 text-xs font-medium text-blue-600 hover:bg-blue-50" href="${esc(objectURL(name, object.key))}" download aria-label="Скачать ${esc(object.key)}">Скачать</a><button class="danger" data-mutation aria-label="Удалить ${esc(object.key)}">Удалить</button>`;
        li.querySelector('button').onclick = async () => {
          if (!confirm('Безвозвратно удалить «' + object.key + '»?')) return;
          await action(async () => { await api(objectURL(name, object.key), { method: 'DELETE' }); signature = ''; await refresh(); toast('Файл удалён'); });
        };
        list.appendChild(li);
      }
      after = j.next_start_after || '';
      button.hidden = !j.truncated;
      detail.querySelector('.list-state').textContent = list.children.length ? 'Показано файлов: ' + list.children.length : 'Здесь пока нет файлов';
    } catch (e) { detail.querySelector('.list-state').textContent = 'Не удалось получить файлы: ' + e.message; }
    finally { button.disabled = false; }
  }
  detail.querySelector('.more').onclick = page;
  await page();
}
function renderNodes(j) {
  $('nodes-count').textContent = 'Этот узел: ' + j.node_id + ' · копий: ' + j.replication_factor;
  $('node-health').textContent = j.nodes_healthy + ' / ' + j.nodes_total;
  $('nodes').innerHTML = (j.nodes || []).map(n => `<button class="node" data-url="${esc(n.url)}"><span class="flex items-center justify-between gap-2"><strong class="truncate text-xs font-semibold text-slate-700">${esc(n.node_id)}</strong><span class="pill ${n.healthy ? 'ok' : 'bad'}">${n.healthy ? 'В сети' : 'Нет связи'}</span></span><span class="mt-2 block break-all text-xs text-slate-500">${esc(n.url)}</span>${n.last_error ? '<span class="mt-1 block break-all text-xs text-red-600">' + esc(n.last_error) + '</span>' : ''}</button>`).join('');
  $('nodes').querySelectorAll('button').forEach(button => button.onclick = () => {
    try {
      const u = new URL(button.dataset.url);
      if (!['https:', 'http:'].includes(u.protocol)) throw Error('Недопустимый URL узла');
      u.pathname = '/ui/'; u.search = ''; u.hash = '';
      window.open(u.href, '_blank', 'noopener');
    } catch (e) { toast(e.message, true); }
  });
}
async function refresh() {
  if (refreshing) return;
  refreshing = true; $('refresh').disabled = true;
  try {
    const j = await (await api('/api/stats')).json();
    renderStats(j); polki = j.polki || []; renderPolki(); renderNodes(j);
    $('connection').hidden = true; $('service-status').textContent = 'Хранилище доступно';
    $('service-dot').classList.replace('bg-amber-500', 'bg-emerald-500');
  } catch (e) {
    $('connection').textContent = 'Не удалось обновить данные. ' + e.message;
    $('connection').hidden = false; $('service-status').textContent = 'Проверьте подключение';
    $('service-dot').classList.replace('bg-emerald-500', 'bg-amber-500');
  } finally { refreshing = false; $('refresh').disabled = false; }
}
async function action(fn) {
  if (busy) return;
  busy = true;
  document.querySelectorAll('[data-mutation]').forEach(b => b.disabled = true);
  try { await fn(); } catch (e) { toast(e.message, true); }
  finally { busy = false; document.querySelectorAll('[data-mutation]').forEach(b => b.disabled = false); }
}
async function create() {
  const name = $('polka').value.trim();
  if (!name) { $('polka').focus(); return; }
  await action(async () => {
    await api('/api/polki', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name }) });
    $('polka').value = ''; openPolka = name; signature = ''; await refresh(); toast('Полка «' + name + '» создана');
  });
}
$('refresh').onclick = () => { signature = ''; refresh(); };
$('create-form').onsubmit = e => { e.preventDefault(); create(); };
$('new-polka').onclick = () => { $('sec-polki').scrollIntoView({ behavior: 'smooth' }); $('polka').focus({ preventScroll: true }); };
$('filter').oninput = () => renderPolki();
function schedule() {
  clearInterval(timer);
  if ($('autorefresh').checked) timer = setInterval(() => { if (!busy && !document.hidden && document.activeElement.tagName !== 'INPUT') refresh(); }, 5000);
}
$('autorefresh').onchange = schedule;
refresh(); schedule();

document.querySelectorAll('.nav-link').forEach(link => link.addEventListener('click', () => {
  document.querySelectorAll('.nav-link').forEach(item => {
    item.classList.toggle('bg-blue-50', item === link);
    item.classList.toggle('text-blue-700', item === link);
    if (item === link) item.setAttribute('aria-current', 'location'); else item.removeAttribute('aria-current');
  });
}));
