LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>Bettr Bot - Login</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72, #2a5298);
            color: white;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .login-container {
            background: rgba(8, 15, 35, 0.9);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 20px;
            padding: 40px;
            max-width: 400px;
            width: 100%;
            backdrop-filter: blur(10px);
            text-align: center;
        }
        .logo { font-size: 3em; margin-bottom: 10px; }
        .subtitle { color: #bdd1ff; margin-bottom: 30px; }
        .form-group { margin-bottom: 20px; text-align: left; }
        label { display: block; margin-bottom: 5px; color: #a8b5d3; }
        input {
            width: 100%;
            padding: 12px;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 8px;
            background: rgba(255,255,255,0.1);
            color: white;
            font-size: 14px;
        }
        input:focus { outline: none; border-color: #2c86ff; }
        .login-btn {
            width: 100%;
            padding: 12px;
            background: #2c86ff;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
        }
        .login-btn:hover { background: #1976d2; }
        .error { color: #ff8a8a; margin-top: 10px; }
        @media (max-width: 480px) {
            .login-container { padding: 20px; margin: 10px; }
        }
        .ai-message, .user-message, .ai-loading-message {
            margin: 10px 0;
            padding: 10px;
            border-radius: 5px;
        }

        .ai-message {
            background: rgba(44, 134, 255, 0.1);
            border-left: 3px solid #2c86ff;
        }

        .user-message {
            background: rgba(255, 255, 255, 0.05);
            border-left: 3px solid #86f093;
            text-align: right;
        }

        .ai-loading-message {
            background: rgba(255, 193, 7, 0.1);
            border-left: 3px solid #ffc107;
            font-style: italic;
        }
    </style>
</head>
<body>
    <div class=\"login-container\">
        <div class=\"logo\">🎰</div>
        <h2>Bettr Bot Dashboard</h2>
        <p class=\"subtitle\">NFL Betting Analysis System</p>
        
        <form method=\"POST\">
            <div class=\"form-group\">
                <label for=\"username\">Username</label>
                <input type=\"text\" id=\"username\" name=\"username\" required>
            </div>
            <div class=\"form-group\">
                <label for=\"password\">Password</label>
                <input type=\"password\" id=\"password\" name=\"password\" required>
            </div>
            <button type=\"submit\" class=\"login-btn\">Login to Dashboard</button>
            {% if error %}
                <div class=\"error\">{{ error }}</div>
            {% endif %}
        </form>
    </div>
</body>
</html>
"""

AI_CHAT_TEMPLATE = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Bettr Bot • AI Chat</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root{
      --bg:#0e2339; --panel:#122b45; --panel2:#0f2841; --text:#e6f0ff;
      --muted:#9db3d1; --accent:#00d4ff; --green:#15d07e; --chip:#183b5e;
    }
    *{box-sizing:border-box}
    body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,Segoe UI,Arial,sans-serif}
    .topbar{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;background:linear-gradient(180deg,#0f2a44,#0b2034)}
    .brand{font-weight:800;letter-spacing:.3px}
    .back a{color:var(--muted);text-decoration:none;border:1px solid #2b4664;padding:6px 10px;border-radius:10px}
    .wrap{display:grid;grid-template-columns:320px 1fr;gap:16px;padding:16px}
    .left{background:var(--panel);border-radius:16px;padding:14px}
    .right{background:var(--panel);border-radius:16px;display:flex;flex-direction:column;min-height:70vh;overflow:hidden}
    h2,h3{margin:0 0 10px 0}
    .game-select{background:var(--panel2);border:1px solid #254664;border-radius:12px;padding:10px;height:52vh;overflow:auto}
    .game{padding:8px;border-radius:10px;cursor:pointer}
    .game:hover{background:#123455}
    .game.active{background:#124c72;border:1px solid #256b92}
    .muted{color:var(--muted);font-size:12px}
    .chips{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 0}
    .chip{background:var(--chip);border:1px solid #2e5378;padding:6px 10px;border-radius:999px;cursor:pointer;font-size:12px}
    .chip:hover{filter:brightness(1.1)}
    .thread{flex:1;padding:16px;overflow:auto;background:linear-gradient(180deg,#102a45,#0e243a)}
    .msg{max-width:820px;margin:10px auto;padding:12px 14px;border-radius:14px;line-height:1.45}
    .me{background:#103a5f;border:1px solid #2b5b83}
    .bot{background:#0f334f;border:1px solid #274f72}
    .pick{padding:8px;border-radius:10px;cursor:pointer;background:#0f2841;border:1px solid #254664;margin-bottom:8px}
    .pick:hover{background:#123455}
    .pct{color:var(--green);font-weight:700}
    .footer{display:flex;gap:8px;padding:12px;border-top:1px solid #1d3f5f;background:var(--panel2)}
    input[type=text]{flex:1;background:#0e2a45;border:1px solid #2c4f73;border-radius:12px;padding:12px;color:var(--text)}
    button{background:linear-gradient(180deg,#0db1d6,#089bbd);border:0;color:#05202c;padding:10px 14px;border-radius:12px;font-weight:700;cursor:pointer}
    .pct{color:var(--green);font-weight:700}
    @media (max-width:900px){.wrap{grid-template-columns:1fr}.left{order:2}.right{order:1}}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="brand">🤖– Bettr Bot • AI Chat</div>
    <div class="back"><a href="/"> ← Back to Dashboard</a></div>
  </div>
  <h3>Model Picks</h3>
  <div class="muted">Click a pick to analyze it immediately.</div>
  <div id="picks" class="game-select" style="height:28vh;margin-bottom:10px;"></div>

  <h3 style="margin-top:6px;">Select a game</h3>
  <div class="muted">Click a matchup, then use quick actions.</div>
  <div class="wrap">
    <aside class="left">
      <h3>Select a game</h3>
      <div class="muted">Click a matchup, then use quick actions or ask anything.</div>
      <div id="games" class="game-select"></div>
      <div class="chips">
        <div class="chip" onclick="quick('analyze')">Analyze game</div>
        <div class="chip" onclick="quick('value5')">Find value bets ≥5%</div>
        <div class="chip" onclick="quick('explain')">Explain the pick</div>
      </div>
    </aside>

    <main class="right">
      <div id="thread" class="thread"></div>
      <div class="footer">
        <input id="msg" type="text" placeholder="Ask about a game, value bets, bankroll, etc." />
        <button id="send">Send</button>
      </div>
    </main>
  </div>

  <script>
    let SELECTED = null;

    function row(g){
        const t = `${g.game}`;
        return `<div class="game ${SELECTED && SELECTED.game_id===g.game_id ? 'active':''}"
                    data-id="${g.game_id}"
                    onclick='pick(${JSON.stringify(g)})'>
                    <div>${t}</div>
                    <div class="muted">${g.date} • ${g.time || ''}</div>
                </div>`;
    }



    async function loadGames(){
      const r = await fetch('/api/games');
      const data = await r.json();
      window.__GAMES = data.slice();
      const box = document.getElementById('games');
      box.innerHTML = data.map(row).join('') || '<div class="muted">No games found.</div>';

      // Preselect via ?game_id=...
      const url = new URL(location.href);
      const gid = new URLSearchParams(window.location.search).get('game_id');
      if (gid && !SELECTED) {  // <-- guard so it runs only once
        const found = (data||[]).find(g => String(g.game_id) === String(gid));
        if (found) { SELECTED = found; pick(found); setTimeout(()=>quick('analyze'),180); }
      }


    }

    function pick(g){
        SELECTED = g;
        const box = document.getElementById('games');
        if (box) {
            box.querySelectorAll('.game').forEach(el => {
            const isActive = el.getAttribute('data-id') === String(g.game_id);
            el.classList.toggle('active', isActive);
            });
        }
        pushBot(`Selected: <b>${g.game}</b><div class="muted">${g.date} • ${g.time || ''}</div>`);

    }




    function pushMe(text){
      const t = document.getElementById('thread');
      t.insertAdjacentHTML('beforeend', `<div class="msg me">${text}</div>`);
      t.scrollTop = t.scrollHeight;
    }
    function pushBot(html){
      const t = document.getElementById('thread');
      t.insertAdjacentHTML('beforeend', `<div class="msg bot">${html}</div>`);
      t.scrollTop = t.scrollHeight;
    }
    async function loadPicks(){
    const r = await fetch('/api/predictions');
    const data = await r.json();
    const box = document.getElementById('picks');
    const row = p => `
      <div class="pick" onclick="goPick('${p.game_id}')">
        <div><b>${p.matchup}</b></div>
        <div class="muted">${p.game_date} ${(p.game_time||'').slice(0,5)} • pick <b>${p.prediction}</b> •
          conf <span class="pct">${(p.confidence*100).toFixed(1)}%</span></div>
      </div>`;
    box.innerHTML = (data||[]).map(row).join('') || '<div class="muted">No upcoming picks.</div>';
  }

  function goPick(gameId){
    // reflect selection in the URL
    const url = new URL(location.href);
    url.searchParams.set('game_id', gameId);
    history.replaceState(null, '', url);

    // if games are already loaded, select + analyze now
    if (window.__GAMES){
      const found = window.__GAMES.find(g => String(g.game_id) === String(gameId));
      if (found){ pick(found); quick('analyze'); return; }
    }
    // else try again after games load
    setTimeout(() => {
      const found = (window.__GAMES||[]).find(g => String(g.game_id) === String(gameId));
      if (found){ pick(found); quick('analyze'); }
    }, 250);
  }


  // kick everything off
  loadPicks();
  loadGames();

    async function callAI(message){
      const payload = { message, game_id: SELECTED && SELECTED.game_id };
      const r = await fetch('/api/ai-chat', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
      const j = await r.json();
      if(!j.ok){ pushBot('Sorry, something went wrong.'); return; }

      if(j.intent==='analysis'){
        const b = j.result?.best_bet || {};
        const edgeTxt = (typeof b.edge === 'number') ? b.edge.toFixed(1) : '—';
        const confTxt = (typeof b.confidence === 'number') ? b.confidence.toFixed(1) : '—';
        const lineTxt = Number.isFinite(b.odds) ? `${b.odds}` : 'n/a';
        pushBot(`Best bet: <b>${b.team || 'n/a'}</b> <span class="pct">${edgeTxt}%</span> edge • conf <span class="pct">${confTxt}%</span><br>Line: <b>${lineTxt}</b> @ ${b.sportsbook || 'best'}`);


        let lines = [];
        if(j.result.summary) lines.push(j.result.summary);
        if(j.result.injuries){
          const i = j.result.injuries;
          const bits = [];
          if(i.home && i.home.qb) bits.push(`Home QB risk: ${i.home.qb}`);
          if(i.away && i.away.qb) bits.push(`Away QB risk: ${i.away.qb}`);
          if(bits.length) lines.push(`<div class="muted">${bits.join(' • ')}</div>`);
        }
        pushBot(lines.join(''));
      }else if(j.intent==='value_bets'){
        const rows = (j.result||[]).map(r => `<div>• ${r.team} ML ${r.odds} — edge <span class="pct">${(r.edge_pct).toFixed(1)}%</span> (p=${(r.model_prob*100).toFixed(0)}%)</div>`);
        pushBot(rows.join('') || 'No edges at that threshold.');
      }else if (j.intent === 'explain_pick')  {
        const team = j.result.team || (j.result.pick || '');
        const list = (j.result.factors || []).map(s => `<li>${s}</li>`).join('');
        const prob = (j.result.model_probability ?? j.result.probability);
        const pct  = (prob != null) ? `${(prob*100).toFixed(1)}%` : '—';
        pushBot(`<div><b>${team}</b> – why the model likes it</div>
                <ul style="margin:6px 0 0 16px;">${list}</ul>
                <div class="muted">Model probability ${pct}</div>`);
      }else if (j.intent === 'explanation') {
        pushBot(j.result.message || 'OK');

      }else{
        pushBot(j.result && j.result.message ? j.result.message : 'How can I help?');
      }
    }

    function quick(kind){
      if(kind==='analyze'){
        if(!SELECTED) return pushBot('Pick a game first.');
        pushMe('Analyze this game');
        callAI('Analyze this game');
      }else if(kind==='value5'){
        pushMe('Find value bets with 5% edge');
        callAI('Find value bets with 5% edge');
      }else if(kind==='explain'){
        if(!SELECTED) return pushBot('Pick a game first.');
        pushMe('Explain the pick');
        callAI('Explain the pick');
      }
    }

    document.getElementById('send').onclick = () => {
      const v = document.getElementById('msg').value.trim();
      if(!v) return;
      pushMe(v);
      document.getElementById('msg').value = '';
      callAI(v);
    };

    loadGames();
  </script>
</body>
</html>
"""


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>Bettr Bot Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72, #2a5298);
            color: #e8eef7;
            min-height: 100vh;
            padding: 10px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        
        .header {
            background: rgba(9, 15, 30, 0.8);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 15px;
            padding: 15px;
            margin-bottom: 15px;
            backdrop-filter: blur(10px);
        }
        .header-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
        .title { font-size: 1.8em; color: #ffcc33; margin-bottom: 5px; }
        .subtitle { color: #bdd1ff; font-size: 13px; }
        .user-info {
            display: flex;
            align-items: center;
            gap: 10px;
            background: rgba(255,255,255,0.05);
            padding: 8px 12px;
            border-radius: 20px;
            flex-wrap: wrap;
        }
        .bankroll { color: #86f093; font-weight: bold; }
        
        .btn {
            padding: 6px 12px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 11px;
            font-weight: 600;
            transition: all 0.3s ease;
            text-decoration: none;
            display: inline-block;
            white-space: nowrap;
        }
        .btn:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-logout { background: #ff4757; color: white; }
        .btn-money { background: #28a745; color: white; }
        .btn-primary { background: #2c86ff; color: white; }
        .btn-success { background: #28a745; color: white; }
        .btn-warning { background: #ffc107; color: #000; }
        .btn-danger { background: #dc3545; color: white; }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 10px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: rgba(8, 15, 35, 0.6);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 12px;
            padding: 15px;
            text-align: center;
            backdrop-filter: blur(5px);
        }
        .stat-value { font-size: 1.5em; font-weight: 800; margin-bottom: 5px; }
        .stat-label { color: #a8b5d3; font-size: 11px; }
        .positive { color: #86f093; }
        .negative { color: #ff8a8a; }
        .neutral { color: #ffe28a; }
        .warning { color: #ff9c7a; }
        
        .panel {
            background: rgba(8, 15, 35, 0.6);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 12px;
            padding: 15px;
            margin-bottom: 15px;
            backdrop-filter: blur(5px);
        }
        .table-viewport { max-height: 360px; overflow-y: auto; }
        .panel h3 {
            color: #ffd54d;
            margin-bottom: 12px;
            font-size: 1.2em;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            border-radius: 8px;
            overflow: hidden;
        }
        th, td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            font-size: 12px;
        }
        th {
            background: rgba(255,255,255,0.1);
            color: #dbe7ff;
            font-weight: 600;
        }
        tr:hover:not(.header-row) { background: rgba(255,255,255,0.05); }
        .right { text-align: right; }
        .center { text-align: center; }
        
        /* Modal (opaque + on top of everything) */
        .modal {
            display: none;
            position: fixed;
            z-index: 3000;
            inset: 0;
            background: rgba(0,0,0,0.85);
        }
        .modal-content {
            background: #0b1330;
            margin: 5% auto;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 12px;
            width: 90%;
            max-width: 520px;
            max-height: 80vh;
            overflow-y: auto;
            position: relative;
            z-index: 3100;
        }

        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 10px; }
        .close { color: #aaa; font-size: 28px; font-weight: bold; cursor: pointer; }
        .close:hover { color: #fff; }
        
        .form-group { margin-bottom: 12px; }
        .form-group label { display: block; margin-bottom: 5px; color: #a8b5d3; font-weight: 600; font-size: 12px; }
        .form-group input, .form-group select, .form-group textarea { width: 100%; padding: 10px; border: 1px solid rgba(255,255,255,0.2); border-radius: 6px; background: rgba(255,255,255,0.1); color: white; font-size: 13px; }
        .form-group input:focus, .form-group select:focus, .form-group textarea:focus { outline: none; border-color: #2c86ff; }

        /* ---------- OVERRIDE: dropdowns legible ---------- */
        select, .form-group select, #edge-filter, #adminModal select, #moneyModal select, #betModal select {
            color: #000 !important; background: #fff !important; border: 1px solid rgba(0,0,0,0.3) !important;
        }
        select option, #edge-filter option, #adminModal select option, #moneyModal select option, #betModal select option { color: #000 !important; background: #fff !important; }
        /* --------------------------------------------------------------------- */
        
        .controls { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
        
        .week-tabs { display: flex; gap: 5px; margin-bottom: 12px; flex-wrap: wrap; }
        .week-tab { padding: 4px 8px; border: 1px solid rgba(255,255,255,0.2); border-radius: 4px; background: rgba(255,255,255,0.1); color: white; cursor: pointer; font-size: 11px; transition: all 0.3s ease; }
        .week-tab.active { background: #ff6b35; border-color: #ff6b35; }
        .week-tab:hover { background: rgba(255,255,255,0.2); }
        
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
        
        .loading { text-align: center; color: #9fb0d8; font-style: italic; padding: 15px; font-size: 12px; }
        
        .alert { padding: 12px; border-radius: 6px; margin: 10px 0; font-size: 12px; }
        .alert-success { background: rgba(40, 167, 69, 0.1); border: 1px solid rgba(40, 167, 69, 0.3); color: #86f093; }
        .alert-info { background: rgba(255, 193, 7, 0.1); border: 1px solid rgba(255, 193, 7, 0.3); color: #ffe28a; }
        .alert-error { background: rgba(220, 53, 69, 0.1); border: 1px solid rgba(220, 53, 69, 0.3); color: #ff8a8a; }
        
        .admin-panel { border: 2px solid #ffd700; background: rgba(255, 215, 0, 0.1); }
        
        /* Mobile Responsive */
        @media (max-width: 768px) {
            body { padding: 5px; }
            .container { margin: 0; }
            .header { padding: 10px; }
            .title { font-size: 1.5em; }
            .user-info { justify-content: center; }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            .grid-2 { grid-template-columns: 1fr; }
            .controls { justify-content: center; }
            .week-tabs { justify-content: center; }
            th, td { padding: 6px 4px; font-size: 11px; }
            .modal-content { margin: 10% auto; width: 95%; }
        }
        
        @media (max-width: 480px) { .stats-grid { grid-template-columns: 1fr; } .user-info { flex-direction: column; gap: 8px; } .header-top { flex-direction: column; text-align: center; } }
    </style>
</head>
<body>

<!-- Floating Predictions Button (ensure above content) -->
<button id=\"aiFab\" title=\"Predictions\"
        style=\"position:fixed;right:16px;bottom:16px;width:56px;height:56px;border-radius:50%;
               font-size:24px;border:none;cursor:pointer;box-shadow:0 6px 16px rgba(0,0,0,.2); z-index:2999;\">
  🤖
</button>

<div id=\"predModal\" style=\"display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:3200;\">
  <div style=\"background:#111;color:#fff;max-width:680px;margin:8% auto;padding:16px;border-radius:12px;\">
    <h3 style=\"margin:0 0 12px\">Model Picks</h3>
    <div id=\"predList\">Loading…</div>
    <div style=\"text-align:right;margin-top:12px\">
      <button onclick=\"closePreds()\" class=\"btn\">Close</button>
    </div>
  </div>
</div>

<div class=\"container\">
    <div class=\"header\">
        <div class=\"header-top\">
            <div>
                <h1 class=\"title\">Bettr Bot Dashboard</h1>
                <p class=\"subtitle\">Live NFL Betting Analysis & Tracking</p>
            </div>
            <div class=\"user-info\">
                <span>{{ user.name }}</span>
                <span class=\"bankroll\">${{ \"%.2f\"|format(user.bankroll) }}</span>
                <button class=\"btn btn-money\" onclick=\"openMoneyModal()\">Money</button>
                {% if user.is_admin %}
                <button class=\"btn btn-warning\" onclick=\"openAdminModal()\">Admin</button>
                {% endif %}
                <button class=\"btn btn-logout\" onclick=\"logout()\">Logout</button>
            </div>
        </div>
    </div>

    <div class=\"stats-grid\">
        <div class=\"stat-card\"><div class=\"stat-value positive\">{{ stats.total_games }}</div><div class=\"stat-label\">Games Tracked</div></div>
        <div class=\"stat-card\"><div class=\"stat-value neutral\">{{ stats.live_odds }}</div><div class=\"stat-label\">Live Odds</div></div>
        <div class=\"stat-card\"><div class=\"stat-value positive\">{{ stats.sportsbooks }}</div><div class=\"stat-label\">Sportsbooks</div></div>
        <div class="stat-card">
        <div class="stat-value warning" id="edgesCount">{{ stats.opportunities }}</div><div class="stat-label">Betting Edges</div></div>
        <div class=\"stat-card\"><div class=\"stat-value neutral\">{{ stats.top_team }}</div><div class=\"stat-label\">Top Team</div></div>
        <div class=\"stat-card\"><div class=\"stat-value\">{{ stats.last_update }}</div><div class=\"stat-label\">Last Update</div></div>
    </div>

    <div class=\"panel\">
        <h3>⚡ Live Betting Opportunities</h3>
        <div class=\"week-tabs\" id=\"week-tabs\"></div>
        <div class=\"controls\">
            <select class=\"btn\" id=\"edge-filter\">
                <option value=\"all\">All Edges</option>
                <option value=\"positive\">Positive Only</option>
                <option value=\"2\">2%+ Edge</option>
                <option value=\"5\">5%+ Edge</option>
            </select>
            <button class=\"btn btn-primary\" onclick=\"refreshAnalysis()\">Refresh</button>
        </div>
        <div id=\"betting-analysis\"><div class=\"loading\">Loading live analysis...</div></div>
    </div>

    <div class=\"grid-2\">
        <div class="panel">
            <h3>🔮 Game Predictions</h3>
            <div class="table-viewport">
                <table>
                    <thead><tr><th>Game</th><th>Date/Time</th><th>Prediction</th><th class="right">Confidence</th></tr></thead>
                    <tbody id="predictions-body"><tr><td colspan="4" class="loading">Loading predictions...</td></tr></tbody>
                </table>
            </div>
        </div>

        <div class="panel">
            <h3>📊 Power Rankings</h3>
            <div class="table-viewport">
                <table>
                    <thead><tr><th>Rank</th><th>Team</th><th>Record</th><th class="right">Power</th><th class="right">Injury Adj</th></tr></thead>
                    <tbody id="rankings-body"><tr><td colspan="5" class="loading">Loading rankings...</td></tr></tbody>
                </table>
            </div>
        </div>
    </div>

    <div class=\"panel\">
        <h3>📈 Your Betting & Money Management</h3>
        <div class=\"controls\">
            <button class=\"btn btn-warning\" onclick=\"openBetModal()\">Add Bet</button>
            <button class=\"btn btn-primary\" onclick=\"openHistoryModal()\">View Bets</button>
            <button class=\"btn btn-success\" onclick=\"openMoneyModal()\">Manage Money</button>
        </div>
        <div class=\"grid-2\">
            <div>
                <h4 style=\"color: #ffd54d; margin-bottom: 8px; font-size: 14px;\">Money Summary</h4>
                <p style=\"font-size: 12px; margin: 3px 0;\">Deposits: <span class=\"positive\">${{ \"%.2f\"|format(user.total_deposits) }}</span></p>
                <p style=\"font-size: 12px; margin: 3px 0;\">Withdrawals: <span class=\"negative\">${{ \"%.2f\"|format(user.total_withdrawals) }}</span></p>
                <p style=\"font-size: 12px; margin: 3px 0;\">Betting P&L: <span class=\"{{ 'positive' if user.betting_profit_loss >= 0 else 'negative' }}\">${{ \"%.2f\"|format(user.betting_profit_loss) }}</span></p>
                <p style=\"font-size: 13px; margin: 5px 0;\"><strong>Balance: <span class=\"bankroll\">${{ \"%.2f\"|format(user.bankroll) }}</span></strong></p>
            </div>
            <div>
                <h4 style=\"color: #ffd54d; margin-bottom: 8px; font-size: 14px;\">Recent Activity</h4>
                <div id=\"recent-activity\" style=\"font-size: 11px;\">Loading recent activity...</div>
            </div>
        </div>
    </div>

    <div class=\"panel\">
        <h3>🔧 System Status</h3>
        <div style=\"display: grid; gap: 5px; font-size: 12px;\">
            <div>● Database: Connected ({{ db_type }})</div>
            <div>● Odds API: {{ 'Active' if stats.live_odds > 0 else 'Inactive' }}</div>
            <div>● Predictions: Running</div>
            <div>● Users: {{ users|length }} accounts active</div>
        </div>
    </div>
</div>

<!-- Money Management Modal -->
<div id=\"moneyModal\" class=\"modal\"><div class=\"modal-content\"><div class=\"modal-header\"><h3>Money Management</h3><span class=\"close\" onclick=\"closeMoneyModal()\">&times;</span></div>
    <form id=\"moneyForm\">
        <div class=\"form-group\"><label>Transaction Type</label><select id=\"transactionType\" required><option value=\"\">Select transaction type...</option><option value=\"deposit\">Deposit Money</option><option value=\"withdraw\">Withdraw Money</option></select></div>
        <div class=\"form-group\"><label>Amount ($)</label><input type=\"number\" id=\"amount\" step=\"0.01\" min=\"0.01\" required placeholder=\"Enter amount\"></div>
        <div class=\"form-group\"><label>Description (Optional)</label><input type=\"text\" id=\"description\" placeholder=\"e.g., Initial bankroll, Profit withdrawal\"></div>
        <div style=\"display:flex;gap:10px;justify-content:flex-end;\"><button type=\"button\" class=\"btn btn-warning\" onclick=\"closeMoneyModal()\">Cancel</button><button type=\"submit\" class=\"btn btn-success\">Process Transaction</button></div>
    </form>
</div></div>

<!-- Bet Modal -->
<div id=\"betModal\" class=\"modal\"><div class=\"modal-content\"><div class=\"modal-header\"><h3>Place a Bet</h3><span class=\"close\" onclick=\"closeBetModal()\">&times;</span></div>
    <div id=\"betStep1\"><h4>Select a Game</h4><div id=\"availableGames\" style=\"max-height:300px;overflow-y:auto;\"><div class=\"loading\">Loading available games...</div></div></div>
    <div id=\"betStep2\" style=\"display:none;\"><h4>Bet Details</h4>
        <form id=\"betForm\">
            <div id=\"selectedGameInfo\" style=\"padding:10px;background:rgba(255,255,255,0.05);border-radius:5px;margin-bottom:10px;\"></div>
            <div class=\"form-group\"><label>Select Team</label><select id=\"betTeam\" required><option value=\"\">Choose team to bet on...</option></select></div>
            <div class=\"form-group\"><label>Amount ($)</label><input type=\"number\" id=\"betAmount\" step=\"0.01\" min=\"0.01\" required placeholder=\"Enter bet amount\"></div>
            <div class=\"form-group\"><label>Odds (Auto from Sportsbook — you can edit)</label><input type=\"text\" id=\"betOdds\" placeholder=\"e.g., +150, -110\"></div>
            <div class=\"form-group\"><label>Sportsbook</label>
                <select id=\"betSportsbook\">
                    <option value=\"DraftKings\">DraftKings</option>
                    <option value=\"FanDuel\">FanDuel</option>
                    <option value=\"BetMGM\">BetMGM</option>
                    <option value=\"Caesars\">Caesars</option>
                    <option value=\"Other\">Other</option>
                </select>
            </div>
            <div style=\"display:flex;gap:10px;justify-content:flex-end;\"><button type=\"button\" class=\"btn btn-warning\" onclick=\"backToGameSelection()\">Back</button><button type=\"submit\" class=\"btn btn-success\">Place Bet</button></div>
        </form>
    </div>
</div></div>

<!-- Betting History Modal -->
<div id=\"historyModal\" class=\"modal\"><div class=\"modal-content\"><div class=\"modal-header\"><h3>Betting History</h3><span class=\"close\" onclick=\"closeHistoryModal()\">&times;</span></div><div id=\"historyContent\"><div class=\"loading\">Loading betting history...</div></div></div></div>

<!-- AI Assistant Panel -->
<div class="panel">
    <h3>🤖 AI Betting Assistant</h3>
    <div class="controls">
        <button class="btn btn-primary" onclick="openAIAssistant()">Ask AI</button>
        <button class="btn btn-success" onclick="getAIPredictions()">Get Today's Picks</button>
        <button class="btn btn-warning" onclick="findValueBets()">Find Value Bets</button>
    </div>
    <div id="ai-results" style="margin-top: 15px; max-height: 400px; overflow-y: auto;">
        <div class="loading">AI Assistant ready. Click a button to get started.</div>
    </div>
</div>

<!-- AI Chat Modal -->
<div id="aiModal" class="modal">
    <div class="modal-content" style="max-width: 800px;">
        <div class="modal-header">
            <h3>AI Betting Assistant</h3>
            <span class="close" onclick="closeAIModal()">&times;</span>
        </div>
        
        <!-- Chat Thread -->
        <div id="ai-chat-thread" style="height: 400px; overflow-y: auto; border: 1px solid rgba(255,255,255,0.1); padding: 15px; margin-bottom: 15px; background: rgba(0,0,0,0.3);">
            <div class="ai-message">
                <strong>AI:</strong> I'm your betting assistant. I've analyzed your 4 years of data and can help you find profitable bets. What would you like to know?
            </div>
        </div>
        
        <!-- Input Area -->
        <div style="display: flex; gap: 10px;">
            <input type="text" id="ai-input" placeholder="Ask about games, teams, or betting strategies..." 
                   style="flex: 1; padding: 10px; background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2); color: white;">
            <button class="btn btn-primary" onclick="sendAIMessage()">Send</button>
        </div>
        
        <!-- Quick Actions -->
        <div style="margin-top: 10px;">
            <button class="btn btn-sm" onclick="askAI('Show me games with 5% edge or better')">5%+ Edge</button>
            <button class="btn btn-sm" onclick="askAI('What are the best bets for today?')">Today's Best</button>
            <button class="btn btn-sm" onclick="askAI('Analyze injuries for upcoming games')">Injury Report</button>
            <button class="btn btn-sm" onclick="askAI('Show me underdog value bets')">Underdog Value</button>
        </div>
    </div>
</div>


<!-- Admin Panel Modal -->
{% if user.is_admin %}
<div id=\"adminModal\" class=\"modal\"><div class=\"modal-content admin-panel\"><div class=\"modal-header\"><h3>Admin Panel</h3><span class=\"close\" onclick=\"closeAdminModal()\">&times;</span></div>
    <div id=\"adminContent\">
        <h4>User Management</h4>
        <div id=\"usersList\" style=\"margin: 15px 0;\">Loading users...</div>
        <h4 style=\"margin-top: 20px; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 15px;\">Season Management</h4>
        <div style=\"margin: 15px 0;\"><button class=\"btn btn-danger\" onclick=\"clearSeasonData()\">Clear All Activity (New Season)</button><p style=\"font-size:11px;color:#ff8a8a;margin-top:5px;\">Warning: This will clear all bet history and transactions for all users</p></div>
        <h4 style=\"margin-top: 20px;\">Adjust User Balance</h4>
        <form id=\"adminAdjustForm\" style=\"margin-top: 10px;\">
            <div class=\"form-group\"><label>Select User</label><select id=\"adminUsername\" required><option value=\"\">Select user...</option></select></div>
            <div class=\"form-group\"><label>Adjustment Amount ($)</label><input type=\"number\" id=\"adminAdjustment\" step=\"0.01\" required placeholder=\"Use negative to subtract\"></div>
            <div class=\"form-group\"><label>Reason</label><input type=\"text\" id=\"adminReason\" required placeholder=\"e.g., Bonus, Correction, etc.\"></div>
            <div style=\"display:flex;gap:10px;justify-content:flex-end;\"><button type=\"submit\" class=\"btn btn-warning\">Adjust Balance</button></div>
        </form>
    </div>
</div></div>
{% endif %}

<script>
    let currentUser = '{{ username }}';
    let currentWeek = 'current';
    let selectedGameData = null; // <- fixed name
    let availableGamesData = [];
    let isAdmin = {{ 'true' if user.is_admin else 'false' }};

    async function loadAllData() {
        await Promise.allSettled([ loadPredictions(), loadRankings(), loadBettingAnalysis(), loadRecentActivity() ]);
    }

    async function loadPredictions() {
        try { const r = await fetch('/api/predictions'); const data = await r.json(); displayPredictions(data); window.__lastPreds = data; }
        catch(e){ document.getElementById('predictions-body').innerHTML = '<tr><td colspan="4" class="loading">Error loading predictions</td></tr>'; }
    }
    async function loadRankings(){ try{ const r=await fetch('/api/rankings'); displayRankings(await r.json()); }catch(e){ document.getElementById('rankings-body').innerHTML='<tr><td colspan="5" class="loading">Error loading rankings</td></tr>'; } }
    
    let analysisCtrl;

    async function loadBettingAnalysis(){
    try{
        if (analysisCtrl) analysisCtrl.abort();
        analysisCtrl = new AbortController();
        const edge = document.getElementById('edge-filter').value;
        const r = await fetch(`/api/betting-analysis?week=${currentWeek}&edge=${edge}`, { signal: analysisCtrl.signal });
        const data = await r.json();
        displayBettingAnalysis(data);
    }catch(e){
        if (e.name === 'AbortError') return; // expected
        document.getElementById('betting-analysis').innerHTML = '<div class="loading">Error loading analysis</div>';
    }
    }
    async function loadRecentActivity(){ try{ const r=await fetch('/api/recent-activity'); displayRecentActivity(await r.json()); }catch(e){ document.getElementById('recent-activity').innerHTML='<div class="loading">Error loading activity</div>'; } }

    function displayPredictions(data){
        const body = document.getElementById('predictions-body');
        if (!Array.isArray(data) || data.length === 0){
            body.innerHTML = '<tr><td colspan="4" class="loading">No upcoming games.</td></tr>';
            return;
        }
        body.innerHTML = data.map(p => `
            <tr>
            <td>${p.matchup}</td>
            <td>${p.game_date} ${(p.game_time || '').slice(0,5)}</td>
            <td>${p.prediction}</td>
            <td class="right">${(p.confidence*100).toFixed(1)}%</td>
            </tr>
        `).join('');
    }



        
    // AI Chat functionality
    let currentGameContext = null;
    let aiChatHistory = [];

    function openAIAssistant(){
        const gid = (window.selectedGameData && selectedGameData.game_id) ? selectedGameData.game_id : '';
        const q = gid ? ('?game_id=' + encodeURIComponent(gid)) : '';
        window.location.href = '/ai' + q;
    }



    function closeAIModal() {
        document.getElementById('aiModal').style.display = 'none';
    }

    async function sendAIMessage() {
        const input = document.getElementById('ai-input');
        const message = input.value.trim();
        if (!message) return;
        
        // Add user message to chat
        addChatMessage('You', message, 'user');
        input.value = '';
        
        // Show loading
        addChatMessage('AI', '...thinking...', 'ai-loading');
        
        try {
            const response = await fetch('/api/ai-chat', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    message: message,
                    game_id: currentGameContext
                })
            });
            
            const data = await response.json();
            
            // Remove loading message
            document.querySelector('.ai-loading')?.remove();
            
            if (data.ok) {
                displayAIResponse(data.result, data.intent);
            } else {
                addChatMessage('AI', 'Sorry, I encountered an error: ' + (data.error || 'Unknown error'), 'ai');
            }
        } catch (error) {
            document.querySelector('.ai-loading')?.remove();
            addChatMessage('AI', 'Network error. Please try again.', 'ai');
        }
    }

    function addChatMessage(sender, message, className) {
        const thread = document.getElementById('ai-chat-thread');
        const div = document.createElement('div');
        div.className = className + '-message';
        div.innerHTML = `<strong>${sender}:</strong> ${message}`;
        thread.appendChild(div);
        thread.scrollTop = thread.scrollHeight;
    }

    function displayAIResponse(result, intent) {
        const thread = document.getElementById('ai-chat-thread');
        const div = document.createElement('div');
        div.className = 'ai-message';
        
        if (intent === 'value_bets' && Array.isArray(result)) {
            // Display value bets
            let html = '<strong>AI:</strong> I found these value betting opportunities:<br><br>';
            result.forEach(bet => {
                const edgeClass = bet.edge_pct > 5 ? 'positive' : bet.edge_pct > 2 ? 'neutral' : 'negative';
                html += `
                    <div style="margin: 10px 0; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 5px;">
                        <strong>${bet.game}</strong> - ${bet.date}<br>
                        Team: ${bet.team}<br>
                        Edge: <span class="${edgeClass}">${bet.edge_pct.toFixed(1)}%</span><br>
                        Odds: ${bet.odds > 0 ? '+' : ''}${bet.odds} @ ${bet.sportsbook}<br>
                        <button class="btn btn-sm btn-success" onclick="placeBetFromAI('${bet.team}', ${bet.odds}, '${bet.sportsbook}')">
                            Place Bet
                        </button>
                    </div>
                `;
            });
            
            if (result.length === 0) {
                html += 'No value bets found with your criteria.';
            }
            
            div.innerHTML = html;
            
        } else if (intent === 'analysis' && result.best_bet) {
            // Display game analysis
            let html = `<strong>AI:</strong> Here's my analysis of ${result.game}:<br><br>`;
            html += `<strong>Win Probabilities:</strong><br>`;
            html += `Home: ${(result.probabilities.home * 100).toFixed(1)}%<br>`;
            html += `Away: ${(result.probabilities.away * 100).toFixed(1)}%<br><br>`;
            
            html += `<strong>Best Bet:</strong><br>`;
            html += `${result.best_bet.team} at ${result.best_bet.odds > 0 ? '+' : ''}${result.best_bet.odds}<br>`;
            html += `Edge: <span class="positive">${result.best_bet.edge.toFixed(1)}%</span><br>`;
            html += `Confidence: ${result.best_bet.confidence}<br>`;
            
            if (result.injuries.home.qb > 0 || result.injuries.away.qb > 0) {
                html += `<br><strong>⚠️ Injury Alert:</strong><br>`;
                if (result.injuries.home.qb > 0) html += 'Home team has QB injury concerns<br>';
                if (result.injuries.away.qb > 0) html += 'Away team has QB injury concerns<br>';
            }
            
            div.innerHTML = html;
            
        } else if (intent === 'explanation' && result.explanation) {
            // Display explanation
            let html = `<strong>AI:</strong> ${result.explanation}<br><br>`;
            html += `<strong>Confidence:</strong> ${result.confidence}<br>`;
            if (result.bet_details) {
                html += `<strong>Recommended Bet:</strong> ${result.bet_details.team} at ${result.bet_details.odds}<br>`;
            }
            div.innerHTML = html;
            
        } else {
            // Generic response
            div.innerHTML = `<strong>AI:</strong> ${JSON.stringify(result)}`;
        }
        
        thread.appendChild(div);
        thread.scrollTop = thread.scrollHeight;
    }

    function askAI(question) {
        document.getElementById('ai-input').value = question;
        sendAIMessage();
    }

    async function getAIPredictions() {
        const resultsDiv = document.getElementById('ai-results');
        resultsDiv.innerHTML = '<div class="loading">Getting AI predictions...</div>';
        
        try {
            const response = await fetch('/api/ai-chat', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({message: 'show me all value bets for today'})
            });
            
            const data = await response.json();
            
            if (data.ok && data.result) {
                let html = '<h4>AI Predictions</h4>';
                
                if (Array.isArray(data.result)) {
                    data.result.forEach(bet => {
                        html += `
                            <div class="alert alert-success" style="margin: 10px 0;">
                                <strong>${bet.game}</strong><br>
                                Bet: ${bet.team} ML<br>
                                Edge: ${bet.edge_pct.toFixed(1)}%<br>
                                Odds: ${bet.odds}
                            </div>
                        `;
                    });
                }
                
                resultsDiv.innerHTML = html;
            }
        } catch (error) {
            resultsDiv.innerHTML = '<div class="alert alert-error">Failed to get predictions</div>';
        }
    }

    // in templates.js area of templates.py
    function findValueBets(){
        openAIAssistant();
        document.getElementById('ai-input').value = 'Find value bets with 5% edge';
        sendAIMessage();
    }



    function placeBetFromAI(team, odds, sportsbook) {
        // Integrate with your existing bet placement system
        openBetModal();
        // Pre-fill the bet form
        setTimeout(() => {
            document.getElementById('betTeam').value = team;
            document.getElementById('betOdds').value = odds;
            document.getElementById('betSportsbook').value = sportsbook;
        }, 100);
    }

    // Auto-analyze when game is selected
    function analyzeGameWithAI(gameId) {
        currentGameContext = gameId;
        openAIAssistant();
        askAI(`Analyze game ${gameId} and tell me the best bet`);
    }

    // Keyboard shortcut
    document.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.key === 'a') {
            e.preventDefault();
            openAIAssistant();
        }
    });

    function displayRankings(data){
        const tbody=document.getElementById('rankings-body'); if(!tbody) return;
        if(!data||!data.length){ tbody.innerHTML='<tr><td colspan="5" class="loading">No rankings available</td></tr>'; return; }
        tbody.innerHTML='';
        data.forEach((t,i)=>{
            const powerClass=t.power_score>=0?'positive':'negative';
            const injAdj = (t.injury_impact&&Number(t.injury_impact)!==0)? Number(-t.injury_impact).toFixed(1) : '-';
            const row=document.createElement('tr');
            row.innerHTML=`<td style="font-size:11px;">${i+1}</td><td style="font-size:11px;">${t.team}</td><td style="font-size:10px;color:#a8b5d3;">${t.record||'0-0'}</td><td class="right ${powerClass}" style="font-size:11px;">${Number(t.power_score).toFixed(1)}</td><td class="right" style="font-size:10px;">${injAdj}</td>`; tbody.appendChild(row);
        });
    }

    let __lastAnalysis = null;
    let edgesRenderLimit = 40;
    function displayBettingAnalysis(data){
        __lastAnalysis = data;
        const container = document.getElementById('betting-analysis');

        // ONE declaration only
        let userBankroll =
            (data && data.user_bankroll != null)
            ? Number(data.user_bankroll)
            : (Array.isArray(data?.opportunities) && data.opportunities[0]?.user_bankroll != null)
            ? Number(data.opportunities[0].user_bankroll)
            : null;

        const maxBetCap   = (data?.max_bet_cap != null) ? Number(data.max_bet_cap) : null;
        const slateBudget = (data?.slate_budget != null) ? Number(data.slate_budget) : null;
        const totalRec    = (data?.total_recommended != null) ? Number(data.total_recommended) : null;


        if (!container) return;

        // Update the stat card
        if (typeof data?.total_found === 'number') {
        const el = document.getElementById('edgesCount');
        if (el) el.textContent = data.total_found;
        }

        // Empty state
        if (!data || !Array.isArray(data.opportunities) || data.opportunities.length === 0) {
        container.innerHTML =
            `<div class="alert alert-info">
            <h4>No Betting Opportunities Found</h4>
            <p>No games meet your current edge threshold.</p>
            <p style="font-size:11px;">Current filter: ${data?.edge_filter ?? 'all'} | Week: ${data?.week ?? 'current'}</p>
            </div>`;
        return;
        }

        const total = data.opportunities.length;
        const slice = data.opportunities.slice(0, edgesRenderLimit);

        // Group by game/date/time
        const groups = new Map();
        for (const opp of slice) {
        const key = `${opp.game}_${opp.date}_${opp.time}`;
        if (!groups.has(key)) groups.set(key, { game: opp.game, date: opp.date, time: opp.time, bets: [] });
        groups.get(key).bets.push(opp);
        }

        let html = '';
        if (userBankroll != null) {
        html += `<div class="alert alert-info">
            Bet sizing uses quarter-Kelly with caps:
            <br>• Bankroll: <strong>$${userBankroll.toFixed(2)}</strong>
            ${maxBetCap != null ? `<br>• Max per bet: <strong>$${maxBetCap.toFixed(2)} (5%)</strong>` : ''}
            ${slateBudget != null ? `<br>• Slate budget: <strong>$${slateBudget.toFixed(2)} (10%)</strong>` : ''}
            ${totalRec != null ? `<br>• Allocated this slate: <strong>$${totalRec.toFixed(2)}</strong>` : ''}
        </div>`;
        }
        html += '<div style="max-height:400px;overflow-y:auto;">';


        for (const g of groups.values()) {
        let betsHTML = '';
        for (const b of g.bets) {
            const edgePct = (b.edge_pct != null) ? Number(b.edge_pct)
                        : (b.edge != null) ? Number(b.edge) * 100
                        : (Number(b.model_prob||0) - Number(b.implied_prob||0)) * 100;

            const eClass = edgePct >= 0 ? 'positive' : 'negative';
            const eText  = (edgePct >= 0 ? '+' : '') + edgePct.toFixed(1) + '%';
            const implied = Math.round((b.implied_prob || 0) * 100);
            const model   = Math.round((b.model_prob   || 0) * 100);
            const amt     = Math.round(Number(b.recommended_amount || 0));
            const odds    = b.odds ?? '';
            const book    = b.sportsbook || '';

            betsHTML += `
            <div style="margin:2px 0;">
                <span style="font-size:12px;">${b.team}</span>
                <span style="font-size:11px;margin-left:6px;">${odds}</span>
                <span class="${eClass}" style="font-size:11px;margin-left:6px;">${eText}</span><br>
                <span style="font-size:10px;color:#a8b5d3;">
                Implied ${implied}% • Model ${model}% • ${book} • Bet <span title="${userBankroll != null ? `Sized from $${userBankroll.toFixed(2)} bankroll` : ''}">${amt}</span>
                </span>
            </div>`;
        }

        html += `<div class="alert alert-success" style="margin-bottom:10px;">
            <div style="display:flex;justify-content:space-between;align-items:start;gap:8px;">
            <div>
                <strong>${g.game}</strong><br>
                <span style="color:#86f093;font-size:11px;">${g.date} • ${g.time}</span>
            </div>
            <div style="text-align:right;">${betsHTML}</div>
            </div>
        </div>`;
        }
        html += '</div>';

        const showing = Math.min(edgesRenderLimit, total);
        html += `<div style="margin-top:10px;padding:8px;background:rgba(255,255,255,0.05);border-radius:5px;">
                <div style="font-size:11px;color:#a8b5d3;">Found ${total} opportunities | Showing ${showing}</div>
                ${showing < total ? `<div style="margin-top:6px;"><button class="btn btn-primary" onclick="showMoreEdges()">Show more</button></div>` : ''}
                </div>`;

        container.innerHTML = html;
    }

    function showMoreEdges(){
    if (!__lastAnalysis) return;
    edgesRenderLimit = Math.min(edgesRenderLimit + 40, __lastAnalysis.opportunities.length);
    displayBettingAnalysis(__lastAnalysis);
    }

    // ---------- BET MODAL ----------
    function openBetModal(){ document.getElementById('betModal').style.display='block'; document.getElementById('betStep1').style.display='block'; document.getElementById('betStep2').style.display='none'; loadAvailableGames(); }
    function closeBetModal(){ document.getElementById('betModal').style.display='none'; const f=document.getElementById('betForm'); if(f) f.reset(); document.getElementById('betStep1').style.display='block'; document.getElementById('betStep2').style.display='none'; selectedGameData=null; }
    function backToGameSelection(){ document.getElementById('betStep1').style.display='block'; document.getElementById('betStep2').style.display='none'; const f=document.getElementById('betForm'); if(f) f.reset(); }

    async function loadAvailableGames(){
        const container=document.getElementById('availableGames'); container.innerHTML='<div class="loading">Loading available games...</div>';
        try{ const r=await fetch('/api/games'); const data=await r.json(); availableGamesData=data||[]; if(!availableGamesData.length){ container.innerHTML='<p class="loading">No upcoming games</p>'; return; }
            container.innerHTML = availableGamesData.map((g,idx)=>`
            <div class="game-selection" style="padding:10px;margin:5px 0;background:rgba(255,255,255,0.05);border-radius:5px;cursor:pointer;" onclick="selectGame(${idx})">
                <div style="display:flex;justify-content:space-between;gap:8px;">
                    <div><strong>${g.game}</strong><br><span style="color:#a8b5d3;font-size:11px;">${g.date} • ${g.time}</span></div>
                    <div style="text-align:right;">${g.teams.map(t=>`<div style="font-size:11px;">${t.team}: <span class="positive">${t.odds>0?`+${t.odds}`:t.odds}</span> <span style="color:#a8b5d3">${t.sportsbook||''}</span></div>`).join('')}</div>
                </div>
            </div>`).join('');
        }catch(e){ container.innerHTML='<div class="alert alert-error">Error loading games</div>'; }
    }

    function selectGame(idx){
        selectedGameData = availableGamesData[idx];
        document.getElementById('betStep1').style.display='none';
        document.getElementById('betStep2').style.display='block';
        document.getElementById('selectedGameInfo').innerHTML=`<strong>${selectedGameData.game}</strong><br><span style="color:#a8b5d3;">${selectedGameData.date} • ${selectedGameData.time}</span>`;
        const teamSelect=document.getElementById('betTeam'); const bookSelect=document.getElementById('betSportsbook'); const oddsInput=document.getElementById('betOdds');
        teamSelect.innerHTML='<option value="">Choose team to bet on...</option>';
        selectedGameData.teams.forEach(t=>{ teamSelect.innerHTML+=`<option value="${t.team}">${t.team}</option>`; });
        // default sportsbook to the one tied to the best line of the first team
        if(selectedGameData.teams[0] && selectedGameData.teams[0].sportsbook){ bookSelect.value = selectedGameData.teams[0].sportsbook; }
        function autofillOdds(){
            const teamName = teamSelect.value; const book = bookSelect.value;
            const team = selectedGameData.teams.find(x=>x.team===teamName);
            if(!team){ oddsInput.value=''; return; }
            const byBook = team.by_book||[]; const match = byBook.find(b=>b.sportsbook===book);
            const line = match ? match.odds : team.odds; // fallback to best
            if(line!==undefined && line!==null){ oddsInput.value = (Number(line)>0?`+${Number(line)}`:`${Number(line)}`); }
        }
        teamSelect.onchange = autofillOdds;
        bookSelect.onchange = autofillOdds;
        // prefill once when entering step 2
        teamSelect.value = selectedGameData.teams[0].team; autofillOdds();
    }

    document.getElementById('betForm').addEventListener('submit', async function(e){
        e.preventDefault(); if(!selectedGameData){ alert('Please select a game first'); return; }
        const team=document.getElementById('betTeam').value; const amount=parseFloat(document.getElementById('betAmount').value); let odds=document.getElementById('betOdds').value.trim(); const sb=document.getElementById('betSportsbook').value;
        if(!odds){ const t = selectedGameData.teams.find(x=>x.team===team); const line = t ? t.odds : 100; odds = (line>0?`+${line}`:`${line}`); }
        const payload={ game:selectedGameData.game, bet_type:`${team} ML`, amount, odds, sportsbook:sb, game_date:selectedGameData.date, game_time:selectedGameData.time, game_id:selectedGameData.game_id };
        try{ const r=await fetch('/api/place-bet',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}); if(r.ok){ alert('Bet placed successfully!'); closeBetModal(); loadRecentActivity(); location.reload(); } else { const err=await r.json(); alert('Error: '+(err.error||'Unknown error')); } }
        catch(err){ alert('Error placing bet: '+err.message); }
    });

    // ---------- HISTORY / ADMIN ----------
    function openHistoryModal(){ document.getElementById('historyModal').style.display='block'; loadFullHistory(); }
    function closeHistoryModal(){ document.getElementById('historyModal').style.display='none'; }
    
    async function loadFullHistory(){
        try{
            const r = await fetch('/api/bet-history');
            const data = await r.json();
            const c = document.getElementById('historyContent');
            if(!data || !data.length){
            c.innerHTML = '<p class="loading">No betting history found</p>';
            return;
            }
            let html = '<div class="table-viewport"><table style="width:100%;font-size:11px;"><thead><tr><th>Date</th><th>Game</th><th>Bet</th><th>Amount</th><th>Result</th><th>P&L</th><th>Actions</th></tr></thead><tbody>';
            data.forEach((b,i)=>{
            const pl = b.profit_loss>0?'positive':b.profit_loss<0?'negative':'neutral';
            const rs = (b.result||'Pending').toLowerCase();
            const sel = (rs==='pending')
                ? `<select id="rslt-${i}" class="btn"><option value="pending" ${rs==='pending'?'selected':''}>Pending</option><option value="win">Win</option><option value="loss">Loss</option><option value="push">Push</option></select>`
                : `<span>${b.result}</span>`;
            const actions = (rs==='pending')
                ? `<button class="btn btn-success" style="font-size:9px;padding:3px 6px;" onclick="settleBetFromRow(${i})">Save</button>
                <button class="btn btn-danger"  style="font-size:9px;padding:3px 6px;" onclick="deleteBet(${i})">Delete</button>`
                : '-';
            html += `<tr>
                <td>${b.date}</td>
                <td>${b.game}</td>
                <td>${b.bet_type} @ ${b.odds}</td>
                <td>${Number(b.amount).toFixed(2)}</td>
                <td>${sel}</td>
                <td class="${pl}">${Number(b.profit_loss).toFixed(2)}</td>
                <td>${actions}</td>
            </tr>`;
            });
            html += '</tbody></table></div>';
            c.innerHTML = html;
        }catch(e){
            document.getElementById('historyContent').innerHTML = '<div class="alert alert-error">Error loading betting history</div>';
        }
        }

    async function settleBetFromRow(i){
        const sel = document.getElementById(`rslt-${i}`);
        if(!sel) return;
        const val = sel.value;
        if(!['win','loss','push','pending'].includes(val)) return;
        if(val === 'pending') return; // nothing to do
        try{
            const r = await fetch('/api/settle-bet', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ bet_index: i, result: val })
            });
            if(r.ok){
            await loadFullHistory();
            await loadRecentActivity();
            location.reload();
            }else{
            const e = await r.json();
            alert('Error: '+(e.error||'Unknown'));
            }
        }catch(err){
            alert('Error settling bet: '+err.message);
        }
        }

        async function deleteBet(i){
        if(!confirm('Delete this pending bet?')) return;
        try{
            const r = await fetch('/api/delete-bet', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ bet_index: i })
            });
            if(r.ok){
            await loadFullHistory();
            await loadRecentActivity();
            location.reload();
            }else{
            const e = await r.json();
            alert('Error: '+(e.error||'Unknown'));
            }
        }catch(err){
            alert('Error deleting bet: '+err.message);
        }
        }


    function openAdminModal(){ if(!isAdmin) return; document.getElementById('adminModal').style.display='block'; loadAdminData(); }
    function closeAdminModal(){ document.getElementById('adminModal').style.display='none'; }
    async function loadAdminData(){ if(!isAdmin) return; try{ const r=await fetch('/api/admin/users'); const users=await r.json(); let html='<table style="width:100%;font-size:11px;"><thead><tr><th>User</th><th>Balance</th><th>Betting P&L</th><th>Bets</th></tr></thead><tbody>'; users.forEach(u=>{ const pl=u.betting_profit_loss>=0?'positive':'negative'; html+=`<tr><td>${u.name} (${u.username})</td><td class="bankroll">${Number(u.bankroll).toFixed(2)}</td><td class="${pl}">${Number(u.betting_profit_loss).toFixed(2)}</td><td>${u.bet_count}</td></tr>`; }); html+='</tbody></table>'; document.getElementById('usersList').innerHTML=html; const dd=document.getElementById('adminUsername'); dd.innerHTML='<option value="">Select user...</option>'; users.forEach(u=>{ const opt=document.createElement('option'); opt.value=u.username; opt.textContent=u.name; dd.appendChild(opt); }); }catch(e){ document.getElementById('usersList').innerHTML='Error loading users'; } }
    document.getElementById('adminAdjustForm') && document.getElementById('adminAdjustForm').addEventListener('submit', async function(e){ e.preventDefault(); const username=document.getElementById('adminUsername').value; const adjustment=parseFloat(document.getElementById('adminAdjustment').value); const reason=document.getElementById('adminReason').value; try{ const r=await fetch('/api/admin/adjust-balance',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username,adjustment,reason})}); if(r.ok){ const res=await r.json(); alert(`Balance adjusted! ${username}: ${Number(res.old_balance).toFixed(2)} → ${Number(res.new_balance).toFixed(2)}`); loadAdminData(); e.target.reset(); } else { const err=await r.json(); alert('Error: '+err.error); } }catch(err){ alert('Error adjusting balance: '+err.message); } });

    // ------- WEEK TABS / PRED MODAL / COMMON -------
    function initializeWeekTabs(){ const weekTabs=document.getElementById('week-tabs'); const weeks=[{id:'current',label:'Current',current:true},{id:'1',label:'Week 1'},{id:'2',label:'Week 2'},{id:'3',label:'Week 3'},{id:'4',label:'Week 4'},{id:'5',label:'Week 5'},{id:'playoffs',label:'Playoffs'},{id:'all',label:'All'}]; weekTabs.innerHTML=''; weeks.forEach(w=>{ const t=document.createElement('div'); t.className=`week-tab ${w.current?'active':''}`; t.textContent=w.label; t.onclick=()=>selectWeek(w.id,t); weekTabs.appendChild(t); }); }
    function selectWeek(weekId, tab){ document.querySelectorAll('.week-tab').forEach(t=>t.classList.remove('active')); if(tab) tab.classList.add('active'); currentWeek=weekId; refreshAnalysis(); }
    function openPreds(){ document.getElementById('predModal').style.display='block'; renderPredList(); }
    function closePreds(){ document.getElementById('predModal').style.display='none'; }
    function renderPredList(){ const list=document.getElementById('predList'); const data=(window.__lastPreds||[]); if(!data.length){ list.innerHTML='<div class="loading">No predictions loaded yet</div>'; return; } list.innerHTML=data.map(p=>`<div style="padding:8px;border-bottom:1px solid rgba(255,255,255,.08)"><div><strong>${p.matchup}</strong></div><div style="color:#a8b5d3;font-size:12px;">${(p.game_date||'TBD')} ${(p.game_time||'').slice(0,5)}</div><div style="display:flex;justify-content:space-between;font-size:13px;margin-top:4px;"><span>${p.prediction}</span><span class="${p.confidence>0.65?'positive':p.confidence>0.55?'neutral':'negative'}">${(p.confidence*100).toFixed(1)}%</span></div></div>`).join(''); }
    document.getElementById('aiFab').addEventListener('click', openPreds);
    function refreshAnalysis(){ loadBettingAnalysis(); loadPredictions(); }
    function logout(){ if(confirm('Are you sure you want to logout?')) window.location.href='/logout'; }
    window.addEventListener('click', e=>{ ['moneyModal','betModal','historyModal','adminModal','predModal'].forEach(id=>{ const el=document.getElementById(id); if(el && e.target===el){ el.style.display='none'; } }); });
    window.addEventListener('keydown', e=>{ if(e.key==='Escape') closePreds(); });

    function decToUS(dec) {
        const d = Number(dec);
        if (!isFinite(d) || d <= 1) return '';
        return d >= 2 ? Math.round((d - 1) * 100) : Math.round(-100 / (d - 1));
    }
    function normalizeOddsToUS(x) {
        const n = Number(x);
        if (!isFinite(n)) return '';
        // Decimal odds usually 1.01–10; otherwise assume already US
        return (n >= 1.01 && n <= 10) ? decToUS(n) : Math.round(n);
    }

    // --- Fetch helper with timeout to avoid hangs ---
    async function fetchJSON(url, opts = {}, timeout = 8000) {
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), timeout);
        try {
        const r = await fetch(url, { ...opts, signal: ctrl.signal });
        if (!r.ok) throw new Error((await r.text()) || `HTTP ${r.status}`);
        return await r.json();
        } finally { clearTimeout(t); }
    }

    // ===== Money modal =====
    function openMoneyModal() {
        const m = document.getElementById('moneyModal');
        if (m) m.style.display = 'block';
    }
    function closeMoneyModal() {
        const m = document.getElementById('moneyModal');
        if (m) m.style.display = 'none';
        document.getElementById('moneyForm')?.reset();
    }
    document.getElementById('moneyForm')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const type = document.getElementById('transactionType')?.value || '';
        const amount = parseFloat(document.getElementById('amount')?.value || '0');
        const description = document.getElementById('description')?.value || ''; // <- matches your HTML
        if (!type || !amount || amount <= 0) return alert('Pick a type and amount > 0');

        try {
        const data = await fetchJSON('/api/money-transaction', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type, amount, description })
        });
        // Update balances on screen
        document.querySelectorAll('.bankroll')
            .forEach(el => el.textContent = `$${Number(data.new_balance || 0).toFixed(2)}`);
        // Refresh activity quietly
        loadRecentActivity?.();
        closeMoneyModal();
        } catch (err) { alert(err.message || 'Transaction failed'); }
    });

    // ===== Recent activity renderer =====
    function displayRecentActivity(items) {
        const box = document.getElementById('recent-activity');
        if (!box) return;
        if (!items || !items.length) {
        box.innerHTML = '<em>No recent activity yet.</em>'; return;
        }
        box.innerHTML = items.slice(0, 50).map(it => {
        const pl = Number(it.profit_loss || 0);
        const plHtml = pl ? `<span class="${pl >= 0 ? 'positive':'negative'}" style="float:right;">
            ${pl >= 0 ? '+' : ''}$${Math.abs(pl).toFixed(2)}</span>` : '';
        return `<div style="display:flex;gap:8px;justify-content:space-between;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.05);">
                    <div><div style="font-size:11px;opacity:.7;">${it.date || ''}</div>
                        <div style="font-size:12px;">${it.description || ''}</div></div>
                    ${plHtml}
                </div>`;
        }).join('');
    }

    // ===== Admin clear =====
    async function clearSeasonData() {
        if (!confirm('Clear ALL usersâ€™ bets and money activity?')) return;
        try {
        const data = await fetchJSON('/api/admin/clear-activity', { method: 'POST' });
        alert(data?.message || 'All activity cleared');
        document.querySelectorAll('.bankroll').forEach(el => el.textContent = '$0.00');
        loadRecentActivity?.();
        } catch (err) { alert(err.message || 'Error clearing activity'); }
    }

    // ===== Patch: make Add Bet odds auto-correct no matter what comes back =====
    // Called inside your existing selectGame() logic
    window._autofillOddsFromTeamAndBook = function(teamObj, bookName) {
        const byBook = teamObj.by_book || [];
        const match = byBook.find(b => b.sportsbook === bookName);
        const raw = (match ? match.odds : teamObj.odds);
        const us = normalizeOddsToUS(raw);
        return (us > 0 ? `+${us}` : `${us}`);
    };
    
   // in DOMContentLoaded init (already calls loadAllData)
    document.addEventListener('DOMContentLoaded', function(){
    try {
        initializeWeekTabs();
        loadAllData();
        getAIPredictions();  // <-- add this line so AI picks render on page load
        document.getElementById('edge-filter').addEventListener('change', refreshAnalysis);
        setInterval(loadAllData, 60000);
    } catch(err){ console.error('Initialization error:', err); }
    });


    let AI_CONTEXT = { game_id: null, team: null };

    function openAIChat(ctx = {}) {
    AI_CONTEXT = { game_id: ctx.game_id || null, team: ctx.team || null };
    const m = document.getElementById('aiChatModal');
    const t = document.getElementById('aiChatThread');
    document.getElementById('aiChatInput').value = '';
    t.innerHTML = `<div class="loading">Hi! Ask me about specific games, value bets, or odds. Try: "Explain ${ctx.team||'the favorite'} in this game".</div>`;
    m.style.display = 'block';
    }

    function closeAIChat(){ document.getElementById('aiChatModal').style.display='none'; }



    function renderAIAnswer(resp){
    const { intent, result } = resp;
    if(intent === 'game_card' && result && !result.error){
        const bHome = result.best_moneyline?.find?.(x=>x.team===result.teams?.home);
        const bAway = result.best_moneyline?.find?.(x=>x.team===result.teams?.away);
        const ph = Math.round((result.probs?.home||0)*100);
        const pa = Math.round((result.probs?.away||0)*100);
        const confTeam = result.pick?.team||'';
        const confPct  = Math.round((result.pick?.confidence||0)*100);
        return `
        <div style="margin:8px 0;">
            <div><strong>${result.matchup}</strong> — ${result.date} ${result.time||''}</div>
            <div style="font-size:12px;color:#a8b5d3;">Home ${ph}% • Away ${pa}% • Pick: <strong>${confTeam}</strong> (${confPct}%)</div>
            <div style="font-size:12px;margin-top:4px;">
            Best ML — Home: ${bHome? (bHome.odds>0?`+${bHome.odds}`:bHome.odds) +' @ '+bHome.sportsbook : 'n/a'}
            • Away: ${bAway? (bAway.odds>0?`+${bAway.odds}`:bAway.odds) +' @ '+bAway.sportsbook : 'n/a'}
            </div>
        </div>`;
    }
    if(intent === 'explain_pick' && result && !result.error){
        const p = Math.round((result.model_probability||0)*100);
        return `
        <div style="margin:8px 0;">
            <div><strong>Why ${result.team}?</strong></div>
            <div style="font-size:12px;color:#a8b5d3;">Model probability: ${p}%.</div>
            <ul style="margin:6px 0 0 16px; font-size:12px;">
            ${(result.factors||[]).map(f=>`<li>${f}</li>`).join('')}
            </ul>
        </div>`;
    }
    if(intent === 'list_value_bets' && Array.isArray(result)){
        const top = result.slice(0,12).map(r=>{
        const e = Number(r.edge_pct||0).toFixed(1);
        return `<div style="padding:6px 0;border-bottom:1px solid rgba(255,255,255,.06);">
            <div><strong>${(r.away_team||'?')} @ ${(r.home_team||'?')}</strong> — ${r.date||''} ${(r.t||'').slice(0,5)}</div>
            <div style="font-size:12px;color:#a8b5d3;">${r.team} ML @ ${r.sportsbook} ${Number(r.odds)>0?`+${r.odds}`:r.odds} • edge ${e}% • bet $${Number(r.recommended_amount||0).toFixed(2)}</div>
        </div>`;
        }).join('');
        return `<div style="margin:8px 0;"><strong>Top value bets</strong>${top || '<div>No edges above threshold.</div>'}</div>`;
    }
    return `<div style="margin:8px 0;">(No formatted result)</div>`;
    }

</script>
</body>
</html>
"""