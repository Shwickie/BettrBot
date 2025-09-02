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
  <title>Bettr Bot • AI Assistant</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #0e2339;
      --panel: #122b45;
      --panel2: #0f2841;
      --text: #e6f0ff;
      --muted: #9db3d1;
      --accent: #00d4ff;
      --green: #15d07e;
      --yellow: #ffcc33;
      --red: #ff6b6b;
      --chip: #183b5e;
      --loading: #4a9eff;
    }
    
    * { box-sizing: border-box; margin: 0; padding: 0; }
    
    body {
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }
    
    .container {
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 16px 20px;
      background: linear-gradient(135deg, #0f2a44, #0b2034);
      border-bottom: 1px solid rgba(255,255,255,0.1);
      box-shadow: 0 2px 10px rgba(0,0,0,0.3);
    }
    
    .brand {
      font-weight: 800;
      font-size: 18px;
      letter-spacing: 0.5px;
      color: var(--yellow);
    }
    
    .back-btn {
      color: var(--muted);
      text-decoration: none;
      border: 1px solid #2b4664;
      padding: 8px 16px;
      border-radius: 8px;
      transition: all 0.3s ease;
      font-size: 14px;
    }
    
    .back-btn:hover {
      background: rgba(255,255,255,0.1);
      transform: translateY(-1px);
    }
    
    .main-content {
      flex: 1;
      display: grid;
      grid-template-columns: 350px 1fr;
      gap: 20px;
      padding: 20px;
      max-height: calc(100vh - 80px);
    }
    
    .sidebar {
      background: var(--panel);
      border-radius: 16px;
      padding: 20px;
      display: flex;
      flex-direction: column;
      box-shadow: 0 4px 20px rgba(0,0,0,0.2);
    }
    
    .chat-area {
      background: var(--panel);
      border-radius: 16px;
      display: flex;
      flex-direction: column;
      box-shadow: 0 4px 20px rgba(0,0,0,0.2);
    }
    
    .section-title {
      font-size: 16px;
      font-weight: 600;
      color: var(--yellow);
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .section-subtitle {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 16px;
    }
    
    .picks-section {
      margin-bottom: 24px;
    }
    
    .picks-container, .games-container {
      background: var(--panel2);
      border: 1px solid #254664;
      border-radius: 12px;
      padding: 12px;
      max-height: 240px;
      overflow-y: auto;
      scrollbar-width: thin;
      scrollbar-color: var(--accent) var(--panel2);
    }
    
    .picks-container::-webkit-scrollbar,
    .games-container::-webkit-scrollbar {
      width: 6px;
    }
    
    .picks-container::-webkit-scrollbar-track,
    .games-container::-webkit-scrollbar-track {
      background: var(--panel2);
    }
    
    .picks-container::-webkit-scrollbar-thumb,
    .games-container::-webkit-scrollbar-thumb {
      background: var(--accent);
      border-radius: 3px;
    }
    
    .pick-item, .game-item {
      padding: 12px;
      border-radius: 8px;
      cursor: pointer;
      margin-bottom: 8px;
      transition: all 0.2s ease;
      border: 1px solid transparent;
    }
    
    .pick-item:hover, .game-item:hover {
      background: rgba(255,255,255,0.05);
      transform: translateY(-1px);
    }
    
    .pick-item.loading, .game-item.loading {
      opacity: 0.6;
      pointer-events: none;
    }
    
    .game-item.active {
      background: #124c72;
      border: 1px solid var(--accent);
      box-shadow: 0 2px 8px rgba(0,212,255,0.2);
    }
    
    .pick-matchup, .game-matchup {
      font-weight: 600;
      font-size: 14px;
      margin-bottom: 4px;
    }
    
    .pick-details, .game-details {
      color: var(--muted);
      font-size: 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    
    .confidence {
      color: var(--green);
      font-weight: 700;
    }
    
    .confidence.medium { color: var(--yellow); }
    .confidence.low { color: var(--red); }
    
    .quick-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 16px;
    }
    
    .quick-btn {
      background: var(--chip);
      border: 1px solid #2e5378;
      padding: 8px 12px;
      border-radius: 20px;
      cursor: pointer;
      font-size: 12px;
      color: var(--text);
      transition: all 0.3s ease;
      white-space: nowrap;
    }
    
    .quick-btn:hover {
      background: var(--accent);
      color: var(--bg);
      transform: translateY(-1px);
      box-shadow: 0 4px 12px rgba(0,212,255,0.3);
    }
    
    .chat-thread {
      flex: 1;
      padding: 20px;
      overflow-y: auto;
      background: linear-gradient(180deg, #102a45, #0e243a);
      scrollbar-width: thin;
      scrollbar-color: var(--accent) transparent;
    }
    
    .chat-thread::-webkit-scrollbar {
      width: 6px;
    }
    
    .chat-thread::-webkit-scrollbar-track {
      background: transparent;
    }
    
    .chat-thread::-webkit-scrollbar-thumb {
      background: var(--accent);
      border-radius: 3px;
    }
    
    .message {
      max-width: 85%;
      margin: 16px 0;
      padding: 16px 18px;
      border-radius: 16px;
      line-height: 1.6;
      animation: fadeIn 0.3s ease;
    }
    
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
    
    .message.user {
      background: #1a4a6b;
      border: 1px solid #2b6b8f;
      margin-left: auto;
      border-bottom-right-radius: 4px;
    }
    
    .message.ai {
      background: #0f334f;
      border: 1px solid #274f72;
      border-bottom-left-radius: 4px;
    }
    
    .message.loading {
      background: var(--panel2);
      border: 1px solid var(--loading);
      animation: pulse 1.5s infinite;
    }
    
    @keyframes pulse {
      0%, 100% { opacity: 0.6; }
      50% { opacity: 1; }
    }
    
    .loading-dots {
      display: inline-block;
    }
    
    .loading-dots::after {
      content: '';
      animation: dots 1.5s infinite;
    }
    
    @keyframes dots {
      0%, 20% { content: '.'; }
      40% { content: '..'; }
      60%, 100% { content: '...'; }
    }
    
    .bet-recommendation {
      background: rgba(21, 208, 126, 0.1);
      border: 1px solid rgba(21, 208, 126, 0.3);
      border-radius: 8px;
      padding: 12px;
      margin: 8px 0;
    }
    
    .bet-recommendation .team {
      font-weight: 700;
      color: var(--green);
    }
    
    .bet-recommendation .edge {
      color: var(--green);
      font-weight: 600;
    }
    
    .injury-alert {
      background: rgba(255, 107, 107, 0.1);
      border: 1px solid rgba(255, 107, 107, 0.3);
      border-radius: 8px;
      padding: 8px;
      margin: 4px 0;
      font-size: 13px;
    }
    
    .value-bet {
      background: rgba(255, 204, 51, 0.1);
      border: 1px solid rgba(255, 204, 51, 0.3);
      border-radius: 8px;
      padding: 10px;
      margin: 4px 0;
    }
    
    .value-bet .edge-pct {
      color: var(--green);
      font-weight: 700;
    }
    
    .chat-input-area {
      border-top: 1px solid rgba(255,255,255,0.1);
      padding: 16px 20px;
      background: var(--panel2);
      border-bottom-left-radius: 16px;
      border-bottom-right-radius: 16px;
    }
    
    .input-container {
      display: flex;
      gap: 12px;
      align-items: flex-end;
    }
    
    .chat-input {
      flex: 1;
      background: var(--bg);
      border: 1px solid #2c4f73;
      border-radius: 12px;
      padding: 12px 16px;
      color: var(--text);
      font-size: 14px;
      resize: none;
      min-height: 44px;
      max-height: 120px;
      line-height: 1.4;
      transition: border-color 0.3s ease;
    }
    
    .chat-input:focus {
      outline: none;
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(0,212,255,0.2);
    }
    
    .send-btn {
      background: linear-gradient(135deg, #0db1d6, #089bbd);
      border: none;
      color: #05202c;
      padding: 12px 20px;
      border-radius: 12px;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.3s ease;
      font-size: 14px;
    }
    
    .send-btn:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(13, 177, 214, 0.4);
    }
    
    .send-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
      box-shadow: none;
    }
    
    .status-indicator {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 12px;
      font-size: 12px;
      color: var(--muted);
    }
    
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
    }
    
    .status-dot.loading {
      background: var(--loading);
      animation: pulse 1s infinite;
    }
    
    .status-dot.error {
      background: var(--red);
    }
    
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      height: 200px;
      color: var(--muted);
      font-size: 14px;
      text-align: center;
    }
    
    .empty-state .icon {
      font-size: 48px;
      margin-bottom: 16px;
      opacity: 0.5;
    }
    
    /* Mobile Responsive */
    @media (max-width: 900px) {
      .main-content {
        grid-template-columns: 1fr;
        gap: 16px;
        padding: 16px;
      }
      
      .sidebar {
        order: 2;
        max-height: 400px;
      }
      
      .chat-area {
        order: 1;
        min-height: 500px;
      }
      
      .message {
        max-width: 95%;
      }
      
      .quick-actions {
        flex-direction: column;
        gap: 6px;
      }
      
      .input-container {
        flex-direction: column;
        gap: 8px;
      }
      
      .send-btn {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="topbar">
      <div class="brand">🤖 Bettr Bot • AI Assistant</div>
      <a href="/" class="back-btn">← Back to Dashboard</a>
    </div>

    <div class="main-content">
      <div class="sidebar">
        <!-- Model Picks Section -->
        <div class="picks-section">
          <div class="section-title">
            📈 Model Picks
          </div>
          <div class="section-subtitle">Click a pick to analyze it immediately</div>
          <div id="picks-container" class="picks-container">
            <div class="empty-state">
              <div class="icon">📊</div>
              <div>Loading model picks...</div>
            </div>
          </div>
        </div>

        <!-- Games Selection Section -->
        <div class="games-section">
          <div class="section-title">
            🏈 Select Game
          </div>
          <div class="section-subtitle">Choose a matchup for detailed analysis</div>
          <div id="games-container" class="games-container">
            <div class="empty-state">
              <div class="icon">🏟️</div>
              <div>Loading games...</div>
            </div>
          </div>

          <!-- Quick Actions -->
          <div class="quick-actions">
            <div class="quick-btn" onclick="quickAction('analyze')">🎯 Analyze Game</div>
            <div class="quick-btn" onclick="quickAction('value5')">💰 Find 5%+ Edges</div>
            <div class="quick-btn" onclick="quickAction('injuries')">🏥 Injury Report</div>
            <div class="quick-btn" onclick="quickAction('explain')">📝 Explain Pick</div>
          </div>
        </div>
      </div>

      <div class="chat-area">
        <div class="status-indicator">
          <div id="status-dot" class="status-dot"></div>
          <span id="status-text">AI Assistant Ready</span>
        </div>
        
        <div id="chat-thread" class="chat-thread">
          <div class="message ai">
            <strong>AI Assistant:</strong> Welcome to Bettr Bot's AI analysis system! I can help you:
            <br><br>
            • Analyze specific games with ML model predictions
            <br>• Find value betting opportunities with statistical edges  
            <br>• Review injury reports and player impacts
            <br>• Explain betting picks and strategies
            <br><br>
            Select a game from the sidebar or ask me anything about NFL betting!
          </div>
        </div>

        <div class="chat-input-area">
          <div class="input-container">
            <textarea 
              id="chat-input" 
              class="chat-input" 
              placeholder="Ask about games, teams, injuries, or betting strategies..."
              rows="1"
            ></textarea>
            <button id="send-btn" class="send-btn">Send</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    let selectedGame = null;
    let isLoading = false;
    let allGames = [];

    // Initialize on page load
    document.addEventListener('DOMContentLoaded', function() {
      initializeChat();
      loadModelPicks();
      loadGames();
      setupInputHandlers();
      
      // Check for game_id in URL
      const urlParams = new URLSearchParams(window.location.search);
      const gameId = urlParams.get('game_id');
      if (gameId) {
        setTimeout(() => {
          selectGameById(gameId);
          quickAction('analyze');
        }, 500);
      }
    });

    function initializeChat() {
      updateStatus('ready', 'AI Assistant Ready');
    }

    function updateStatus(type, message) {
      const dot = document.getElementById('status-dot');
      const text = document.getElementById('status-text');
      
      dot.className = `status-dot ${type}`;
      text.textContent = message;
    }

    async function loadModelPicks() {
      try {
        updateStatus('loading', 'Loading model picks...');
        
        const response = await fetch('/api/predictions');
        const picks = await response.json();
        
        const container = document.getElementById('picks-container');
        
        if (!picks || picks.length === 0) {
          container.innerHTML = `
            <div class="empty-state">
              <div class="icon">📭</div>
              <div>No upcoming picks available</div>
            </div>
          `;
          return;
        }

        container.innerHTML = picks.slice(0, 8).map(pick => {
          const confidence = pick.confidence * 100;
          const confClass = confidence > 65 ? 'high' : confidence > 55 ? 'medium' : 'low';
          
          return `
            <div class="pick-item" onclick="selectGameByMatchup('${pick.game_id}', '${pick.matchup}')">
              <div class="pick-matchup">${pick.matchup}</div>
              <div class="pick-details">
                <span>${pick.game_date} • ${(pick.game_time || '').slice(0,5)}</span>
                <span>
                  Pick: <strong>${pick.prediction}</strong> 
                  (<span class="confidence ${confClass}">${confidence.toFixed(1)}%</span>)
                </span>
              </div>
            </div>
          `;
        }).join('');
        
        updateStatus('ready', 'Model picks loaded');
        
      } catch (error) {
        console.error('Failed to load picks:', error);
        document.getElementById('picks-container').innerHTML = `
          <div class="empty-state">
            <div class="icon">⚠️</div>
            <div>Failed to load picks</div>
          </div>
        `;
      }
    }

    async function loadGames() {
      try {
        const response = await fetch('/api/games');
        const games = await response.json();
        allGames = games || [];
        
        const container = document.getElementById('games-container');
        
        if (allGames.length === 0) {
          container.innerHTML = `
            <div class="empty-state">
              <div class="icon">📅</div>
              <div>No upcoming games</div>
            </div>
          `;
          return;
        }

        container.innerHTML = allGames.map(game => {
        const dateRaw = game.game_date || game.date || "";
        const timeRaw = game.start_time_local || game.time || "";

        const displayDate = String(dateRaw).slice(0, 10);  // "YYYY-MM-DD"
        const displayTime = String(timeRaw).slice(0, 5);   // "HH:MM"

        const books =
            new Set([
            ...((game.teams?.[0]?.by_book || []).map(b => b.sportsbook)),
            ...((game.teams?.[1]?.by_book || []).map(b => b.sportsbook)),
            ]).size;

        return `
            <div class="game-item" data-game-id="${game.game_id}" onclick="selectGame('${game.game_id}')">
            <div class="game-matchup">${game.game}</div>
            <div class="game-details">
                <span>${displayDate} • ${displayTime}</span>
                <span>${books} ${books === 1 ? 'book' : 'books'}</span>
            </div>
            </div>
        `;
        }).join('');

        
      } catch (error) {
        console.error('Failed to load games:', error);
        document.getElementById('games-container').innerHTML = `
          <div class="empty-state">
            <div class="icon">❌</div>
            <div>Error loading games</div>
          </div>
        `;
      }
    }

    function selectGame(gameId) {
      // Update visual selection
      document.querySelectorAll('.game-item').forEach(item => {
        item.classList.remove('active');
      });
      
      const selectedItem = document.querySelector(`[data-game-id="${gameId}"]`);
      if (selectedItem) {
        selectedItem.classList.add('active');
        selectedGame = allGames.find(g => g.game_id === gameId);
        
        if (selectedGame) {
          addMessage('user', `Selected: ${selectedGame.game}`);
          updateStatus('ready', `Game selected: ${selectedGame.game}`);
          
          // Update URL without refresh
          const url = new URL(window.location);
          url.searchParams.set('game_id', gameId);
          history.replaceState(null, '', url);
        }
      }
    }

    function selectGameById(gameId) {
      const game = allGames.find(g => g.game_id === gameId);
      if (game) {
        selectGame(gameId);
      }
    }

    function selectGameByMatchup(gameId, matchup) {
      selectGame(gameId);
      setTimeout(() => quickAction('analyze'), 100);
    }

    function quickAction(action) {
      if (isLoading) return;
      
      let message = '';
      
      switch (action) {
        case 'analyze':
          if (!selectedGame) {
            addMessage('ai', 'Please select a game first to analyze.');
            return;
          }
          message = 'Analyze this game';
          break;
        case 'value5':
          message = 'Find value bets with 5% or higher edge';
          break;
        case 'injuries':
          message = 'Show current injury report';
          break;
        case 'explain':
          if (!selectedGame) {
            addMessage('ai', 'Please select a game first to explain the pick.');
            return;
          }
          message = 'Explain why the model likes this pick';
          break;
      }
      
      if (message) {
        addMessage('user', message);
        sendMessage(message);
      }
    }

    function setupInputHandlers() {
      const input = document.getElementById('chat-input');
      const button = document.getElementById('send-btn');
      
      // Auto-resize textarea
      input.addEventListener('input', function() {
        this.style.height = 'auto';
        this.style.height = Math.min(this.scrollHeight, 120) + 'px';
      });
      
      // Send on Enter (but allow Shift+Enter for new lines)
      input.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          handleSend();
        }
      });
      
      button.addEventListener('click', handleSend);
    }

    function handleSend() {
      if (isLoading) return;
      
      const input = document.getElementById('chat-input');
      const message = input.value.trim();
      
      if (!message) return;
      
      addMessage('user', message);
      input.value = '';
      input.style.height = 'auto';
      
      sendMessage(message);
    }

    async function sendMessage(message) {
      if (isLoading) return;
      
      setLoading(true);
      addLoadingMessage();
      
      try {
        const response = await fetch('/api/ai-chat-comprehensive', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: message,
            game_id: selectedGame?.game_id
          })
        });
        
        const data = await response.json();
        removeLoadingMessage();
        
        if (data.ok) {
          displayAIResponse(data);
        } else {
          addMessage('ai', `Sorry, I encountered an error: ${data.error || 'Unknown error'}`);
        }
        
      } catch (error) {
        removeLoadingMessage();
        addMessage('ai', 'Network error. Please check your connection and try again.');
        console.error('Chat error:', error);
      } finally {
        setLoading(false);
      }
    }

    function displayAIResponse(data) {
      const { intent, result } = data;
      
      if (intent === 'analysis' && result) {
        displayGameAnalysis(result);
      } else if (intent === 'value_bets' && Array.isArray(result)) {
        displayValueBets(result);
      } else if (intent === 'injury_report' && result) {
        displayInjuryReport(result);
      } else if (result?.message) {
        addMessage('ai', result.message);
      } else {
        addMessage('ai', 'I processed your request, but I don\'t have specific information to share right now.');
      }
    }

    // Enhanced displayGameAnalysis function for templates.py
    function displayGameAnalysis(analysis) {
      const homeProb = (analysis.probabilities?.home * 100) || 50;
      const awayProb = (analysis.probabilities?.away * 100) || 50;
      const bestBet = analysis.best_bet || {};
      
      let html = `<strong>🎯 Game Analysis: ${analysis.game}</strong><br><br>`;
      
      // Always show the detailed AI summary first if available
      if (analysis.summary) {
        html += `${analysis.summary}<br><br>`;
      }
      
      html += `<strong>📊 Model Probabilities:</strong><br>`;
      html += `• Home: ${homeProb.toFixed(1)}%<br>`;
      html += `• Away: ${awayProb.toFixed(1)}%<br>`;
      html += `• Confidence: ${((analysis.confidence_score || 0.7) * 100).toFixed(0)}%<br><br>`;
      
      if (bestBet.team) {
        html += `<div class="bet-recommendation">`;
        html += `<strong>💰 Best Betting Value:</strong><br>`;
        html += `<span class="team">${bestBet.team}</span> at `;
        html += `<strong>${bestBet.odds > 0 ? '+' : ''}${bestBet.odds}</strong><br>`;
        html += `Edge: <span class="edge">${(bestBet.edge_pct || bestBet.edge * 100).toFixed(1)}%</span> • `;
        html += `Model: <span class="confidence">${(bestBet.model_prob * 100).toFixed(1)}%</span><br>`;
        html += `Sportsbook: ${bestBet.sportsbook || 'Best available'}`;
        html += `</div><br>`;
      } else {
        html += `<div class="value-bet">`;
        html += `<strong>📈 Market Assessment:</strong><br>`;
        html += `No significant betting edge detected in current lines.`;
        html += `</div><br>`;
      }
      
      // Key factors from model
      if (analysis.key_factors && analysis.key_factors.length > 0) {
        html += `<strong>🔑 Key Model Factors:</strong><br>`;
        analysis.key_factors.forEach(factor => {
          html += `• ${factor}<br>`;
        });
        html += `<br>`;
      }
      
      // Show injury alerts for relevant teams only
      const injuries = analysis.injuries || {};
      if (injuries.home?.qb > 0 || injuries.away?.qb > 0) {
        html += `<div class="injury-alert">`;
        html += `<strong>⚠️ Injury Impact:</strong><br>`;
        if (injuries.home?.qb > 0) html += `• Home team: QB injury concerns<br>`;
        if (injuries.away?.qb > 0) html += `• Away team: QB injury concerns<br>`;
        html += `</div>`;
      }
      
      addMessage('ai', html);
    }

    function displayValueBets(bets) {
      if (!bets || bets.length === 0) {
        addMessage('ai', 'No value bets found with the current criteria.');
        return;
      }
      
      let html = `<strong>💰 Value Betting Opportunities</strong><br><br>`;
      html += `Found ${bets.length} opportunities:<br><br>`;
      
      bets.slice(0, 8).forEach(bet => {
        const game = bet.away_team && bet.home_team ? 
          `${bet.away_team} @ ${bet.home_team}` : 
          (bet.game || bet.matchup || 'TBD');
        
        const edgePct = bet.edge_pct != null ? bet.edge_pct : 
          ((bet.model_prob - bet.implied_prob) * 100);
        
        const odds = bet.odds > 0 ? `+${bet.odds}` : `${bet.odds}`;
        
        html += `<div class="value-bet">`;
        html += `<strong>${game}</strong><br>`;
        html += `${bet.team} ML ${odds} @ ${bet.sportsbook}<br>`;
        html += `Edge: <span class="edge-pct">${edgePct.toFixed(1)}%</span> • `;
        html += `Model: ${(bet.model_prob * 100).toFixed(0)}% • `;
        html += `Bet: ${(bet.recommended_amount || 0).toFixed(0)}`;
        html += `</div>`;
      });
      
      addMessage('ai', html);
    }

    function displayInjuryReport(report) {
      const injuries = report.injuries || [];
      
      if (injuries.length === 0) {
        const msg = report.filtered_to_game ? 
          'No active injuries found for the selected game teams.' :
          'No active injuries found in the current report.';
        addMessage('ai', msg);
        return;
      }
      
      let html = `<strong>🏥 Injury Report</strong>`;
      
      if (report.filtered_to_game) {
        html += ` (Selected Game Teams)`;
      }
      
      html += `<br><br>Total active injuries: ${injuries.length}<br><br>`;
      
      // Group by team and only show meaningful injuries
      const byTeam = {};
      injuries.forEach(injury => {
        // Only include significant injury designations
        if (['OUT', 'DOUBTFUL', 'QUESTIONABLE', 'IR', 'INJURED RESERVE'].includes(injury.designation.toUpperCase())) {
          if (!byTeam[injury.team]) byTeam[injury.team] = [];
          byTeam[injury.team].push(injury);
        }
      });
      
      if (Object.keys(byTeam).length === 0) {
        addMessage('ai', 'No significant injuries found for the selected criteria.');
        return;
      }
      
      // Sort teams by injury impact
      const teamImpacts = report.team_impacts || {};
      const sortedTeams = Object.keys(byTeam).sort((a, b) => 
        (teamImpacts[b] || 0) - (teamImpacts[a] || 0)
      );
      
      sortedTeams.slice(0, 8).forEach(team => {
        html += `<div class="injury-alert">`;
        html += `<strong>${team}:</strong><br>`;
        byTeam[team].slice(0, 6).forEach(injury => {
          const severity = injury.designation.toUpperCase();
          const severityIcon = severity === 'OUT' || severity === 'IR' ? '🚫' : 
                              severity === 'DOUBTFUL' ? '❓' : '⚠️';
          html += `${severityIcon} ${injury.player} (${injury.position}) - ${injury.designation}`;
          if (injury.detail) html += ` - ${injury.detail}`;
          html += `<br>`;
        });
        html += `</div><br>`;
      });
      
      addMessage('ai', html);
    }

    function addMessage(sender, content) {
      const thread = document.getElementById('chat-thread');
      const messageDiv = document.createElement('div');
      messageDiv.className = `message ${sender}`;
      
      if (sender === 'user') {
        messageDiv.innerHTML = `<strong>You:</strong> ${content}`;
      } else {
        messageDiv.innerHTML = content.startsWith('<strong>') ? content : `<strong>AI:</strong> ${content}`;
      }
      
      thread.appendChild(messageDiv);
      thread.scrollTop = thread.scrollHeight;
    }

    function addLoadingMessage() {
      const thread = document.getElementById('chat-thread');
      const messageDiv = document.createElement('div');
      messageDiv.className = 'message ai loading';
      messageDiv.id = 'loading-message';
      messageDiv.innerHTML = `<strong>AI:</strong> <span class="loading-dots">Analyzing</span>`;
      
      thread.appendChild(messageDiv);
      thread.scrollTop = thread.scrollHeight;
    }

    function removeLoadingMessage() {
      const loading = document.getElementById('loading-message');
      if (loading) {
        loading.remove();
      }
    }

    function setLoading(loading) {
      isLoading = loading;
      const button = document.getElementById('send-btn');
      const input = document.getElementById('chat-input');
      
      button.disabled = loading;
      button.textContent = loading ? 'Thinking...' : 'Send';
      
      if (loading) {
        updateStatus('loading', 'Processing your request...');
      } else {
        updateStatus('ready', selectedGame ? `Selected: ${selectedGame.game}` : 'AI Assistant Ready');
      }
    }

    // Keyboard shortcuts
    document.addEventListener('keydown', function(e) {
      if (e.ctrlKey || e.metaKey) {
        switch(e.key) {
          case '1':
            e.preventDefault();
            quickAction('analyze');
            break;
          case '2':
            e.preventDefault();
            quickAction('value5');
            break;
          case '3':
            e.preventDefault();
            quickAction('injuries');
            break;
        }
      }
    });
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
            const response = await fetch('/api/ai-chat-comprehensive', {
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
                const title =
                    bet.game ||
                    (bet.away_team && bet.home_team ? `${bet.away_team} @ ${bet.home_team}` :
                    (bet.matchup || 'Matchup TBD'));

                const edgePct = (bet.edge_pct != null)
                    ? Number(bet.edge_pct)
                    : ((Number(bet.model_prob || 0) - Number(bet.implied_prob || 0)) * 100);

                const oddsTxt = (Number(bet.odds) > 0 ? `+${bet.odds}` : `${bet.odds ?? ''}`);
                const edgeClass = edgePct > 5 ? 'positive' : edgePct > 2 ? 'neutral' : 'negative';

                html += `
                    <div style="margin: 10px 0; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 5px;">
                    <strong>${title}</strong> - ${bet.date || ''}<br>
                    Team: ${bet.team}<br>
                    Edge: <span class="${edgeClass}">${edgePct.toFixed(1)}%</span><br>
                    Odds: ${oddsTxt} ${bet.sportsbook ? `@ ${bet.sportsbook}` : ''}
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

    // Add this to your templates.py JavaScript section

    async function getAIPredictions() {
        const resultsDiv = document.getElementById('ai-results');
        resultsDiv.innerHTML = '<div class="loading">Getting personalized betting recommendations...</div>';
        
        try {
            const response = await fetch('/api/ai-betting-recommendations');
            const data = await response.json();
            
            if (data.ok && data.result) {
                const { recommendations, bankroll, total_recommended, remaining_budget, risk_level } = data.result;
                
                let html = '<h4>🎯 Personalized Betting Recommendations</h4>';
                html += `<div class="alert alert-info">
                    Bankroll: <strong>$${bankroll}</strong> • 
                    Recommended Today: <strong>$${total_recommended}</strong> • 
                    Remaining Budget: <strong>$${remaining_budget}</strong> • 
                    Risk Level: <strong>${risk_level}</strong>
                </div>`;
                
                if (recommendations.length === 0) {
                    html += '<div class="alert alert-info">No high-confidence betting opportunities found for your bankroll today.</div>';
                } else {
                    recommendations.forEach(rec => {
                        const isValueBet = rec.type === 'value_bet';
                        const bgClass = isValueBet ? 'alert-success' : 'alert-info';
                        
                        html += `
                            <div class="${bgClass}" style="margin: 10px 0;">
                                <div style="display: flex; justify-content: space-between; align-items: start;">
                                    <div>
                                        <strong>${rec.game}</strong><br>
                                        <strong>Bet: ${rec.team}</strong><br>
                                        ${rec.odds !== 'Check sportsbook' ? `Odds: ${rec.odds > 0 ? '+' : ''}${rec.odds} @ ${rec.sportsbook}` : 'Odds: Check sportsbooks'}<br>
                                        <small>${rec.reason}</small>
                                    </div>
                                    <div style="text-align: right;">
                                        <strong>Stake: $${rec.recommended_stake}</strong><br>
                                        ${rec.potential_profit ? `Profit: $${rec.potential_profit}<br>` : ''}
                                        <span class="confidence ${rec.confidence.toLowerCase()}">${rec.confidence} Confidence</span>
                                        ${isValueBet ? '<br><span style="color: #15d07e;">📈 Value Bet</span>' : '<br><span style="color: #4a9eff;">🎯 Model Pick</span>'}
                                    </div>
                                </div>
                                <button class="btn btn-sm btn-success" onclick="placeBetFromRecommendation('${rec.team}', '${rec.odds}', '${rec.sportsbook}', ${rec.recommended_stake})" 
                                        style="margin-top: 8px;">
                                    Place This Bet
                                </button>
                            </div>
                        `;
                    });
                }
                
                html += `<div class="alert alert-info" style="margin-top: 15px;">
                    <small><strong>Risk Management:</strong> Recommendations use conservative Kelly criterion (25% of optimal) 
                    with 5% max per bet and 10% daily budget limits.</small>
                </div>`;
                
                resultsDiv.innerHTML = html;
            } else {
                resultsDiv.innerHTML = '<div class="alert alert-error">Failed to get recommendations: ' + (data.error || 'Unknown error') + '</div>';
            }
        } catch (error) {
            resultsDiv.innerHTML = '<div class="alert alert-error">Network error getting recommendations</div>';
        }
    }

    // Add this helper function for bet placement integration
    function placeBetFromRecommendation(team, odds, sportsbook, amount) {
        openBetModal();
        
        // Pre-fill the bet form after a short delay to ensure modal is open
        setTimeout(() => {
            const teamSelect = document.getElementById('betTeam');
            const amountInput = document.getElementById('betAmount');
            const oddsInput = document.getElementById('betOdds');
            const bookSelect = document.getElementById('betSportsbook');
            
            if (teamSelect) {
                // Find the option that matches the team name
                for (let option of teamSelect.options) {
                    if (option.text.includes(team) || option.value.includes(team)) {
                        option.selected = true;
                        break;
                    }
                }
            }
            
            if (amountInput) amountInput.value = amount;
            if (oddsInput && odds !== 'Check sportsbook') oddsInput.value = odds;
            if (bookSelect && sportsbook !== 'Various') bookSelect.value = sportsbook;
        }, 200);
    }

    // Update your existing getAIPredictions call in the dashboard initialization
    document.addEventListener('DOMContentLoaded', function(){
        try {
            initializeWeekTabs();
            loadAllData();
            getAIPredictions(); // This now loads personalized recommendations
            document.getElementById('edge-filter').addEventListener('change', refreshAnalysis);
            setInterval(loadAllData, 60000);
        } catch(err){ 
            console.error('Initialization error:', err); 
        }
    });

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