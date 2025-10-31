#---------- LIBRARYS ----------

from flask import Flask, send_file, request, jsonify, redirect, render_template
from aut import get_real_leads
import asyncio


#---------- VARIABLES ----------

app = Flask(__name__)


#---------- GET PAGE ----------

@app.route("/", methods = ["GET"])
def get_index():
    return render_template("index.html")


#---------- POST PAGE ----------

@app.route("/add_leads", methods=["POST"])
def add_leads():
    data = request.json
    query = data.get("query")
    leads = asyncio.run(get_real_leads(query))

    if len(leads) > 0:
        return jsonify({"message": leads})
    else:
        return jsonify({"error": "Mi dispiace ma non ho trovato nulla"})


#---------- START SERVER ----------

if __name__ == "__main__":
    app.run(debug=True)