# templates.py - Minimal templates for cloud deployment
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>Login - Bettr Bot</title></head>
<body>
    <h2>Bettr Bot Login</h2>
    <form method="post">
        Username: <input name="username" type="text" required><br><br>
        Password: <input name="password" type="password" required><br><br>
        <button type="submit">Login</button>
    </form>
    {% if error %}<p style="color:red">{{ error }}</p>{% endif %}
</body>
</html>
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>Bettr Bot Dashboard</title></head>
<body>
    <h1>Bettr Bot Dashboard</h1>
    <p>Welcome {{ username }}!</p>
    <p>Bankroll: ${{ user.bankroll }}</p>
    <a href="/logout">Logout</a>
</body>
</html>
"""

AI_CHAT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>AI Chat - Bettr Bot</title></head>
<body>
    <h1>AI Chat</h1>
    <div id="chat">Chat functionality will be implemented here.</div>
    <a href="/">Back to Dashboard</a>
</body>
</html>
"""
