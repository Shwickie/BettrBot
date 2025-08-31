import sqlite3
db = r"E:\Bettr Bot\betting-bot\data\betting.db"
con = sqlite3.connect(db)
c = con.cursor()
c.execute("PRAGMA journal_mode=WAL")
c.execute("PRAGMA busy_timeout=60000")
c.execute("PRAGMA synchronous=NORMAL")
con.commit()
con.close()
print("DB pragmas set.")
