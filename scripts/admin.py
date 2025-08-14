from flask import Flask, render_template_string
import sqlite3

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Ingestion Logs</title>
    <style>
        table {border-collapse: collapse; width: 80%; margin: 20px auto;}
        th, td {border: 1px solid #ddd; padding: 8px; text-align: center;}
        th {background-color: #f2f2f2;}
    </style>
</head>
<body>
    <h1 style="text-align:center;">Job Import Logs</h1>
    <table>
        <tr>
            <th>ID</th>
            <th>Source File</th>
            <th>Status</th>
            <th>Timestamp</th>
        </tr>
        {% for log in logs %}
        <tr>
            <td>{{ log[0] }}</td>
            <td>{{ log[1] }}</td>
            <td>{{ log[2] }}</td>
            <td>{{ log[3] }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""

def get_logs():
    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()
    cur.execute("SELECT * FROM ingestion_logs ORDER BY timestamp DESC")
    logs = cur.fetchall()
    conn.close()
    return logs

@app.route('/')
def index():
    logs = get_logs()
    return render_template_string(HTML, logs=logs)

if __name__ == "__main__":
    app.run(debug=True)
