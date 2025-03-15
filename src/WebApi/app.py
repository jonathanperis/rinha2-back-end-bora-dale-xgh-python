from flask import Flask, request, jsonify
import os
import psycopg2
import psycopg2.pool
import json
from datetime import datetime

app = Flask(__name__)

# Predefined clients (id -> limite)
clientes = {
    1: 100000,
    2: 80000,
    3: 1000000,
    4: 10000000,
    5: 500000,
}

# Get DATABASE_URL from environment variables.
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("DATABASE_URL env var is not set")

# Create a connection pool
pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL)

@app.route("/healthz", methods=["GET"])
def healthz():
    return "Healthy", 200

@app.route("/clientes/<int:client_id>/extrato", methods=["GET"])
def get_extrato(client_id):
    if client_id not in clientes:
        return "Client not found", 404

    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM GetSaldoClienteById(%s)", (client_id,))
            row = cur.fetchone()
            if row is None:
                return "Extrato not found", 404

            total, db_limite, data_extrato, transacoes_json = row

            # Convert transactions and rename keys
            transacoes = []
            if transacoes_json:
                try:
                    raw_transacoes = json.loads(transacoes_json)
                    for t in raw_transacoes:
                        transacao = {
                            "valor": t["Valor"],
                            "tipo": t["Tipo"],
                            "descricao": t["Descricao"]
                        }
                        transacoes.append(transacao)
                except Exception as e:
                    print(f"Error parsing transactions: {e}")
                    transacoes = []

            extrato = {
                "saldo": {
                    "total": total,
                    "limite": db_limite,
                    "data_extrato": data_extrato.isoformat() if isinstance(data_extrato, datetime) else data_extrato
                },
                "ultimas_transacoes": transacoes
            }

            return jsonify(extrato)
    except Exception as e:
        return f"Error reading from database: {e}", 500
    finally:
        pool.putconn(conn)

@app.route("/clientes/<int:client_id>/transacoes", methods=["POST"])
def post_transacao(client_id):
    if client_id not in clientes:
        return "Client not found", 404

    data = request.get_json()
    if not data:
        return "Invalid JSON payload", 400

    valor = data.get("valor")
    tipo = data.get("tipo")
    descricao = data.get("descricao")

    # Validate transaction input format
    if not is_transacao_valid(valor, tipo, descricao):
        return "Invalid transaction data", 422

    # For debit transactions, check the business rule:
    # o débito não pode deixar o saldo menor que -limite
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            # Get the current balance
            cur.execute('SELECT "SaldoInicial" FROM public."Clientes" WHERE "Id" = %s', (client_id,))
            row = cur.fetchone()
            if row is None:
                return "Client not found", 404
            current_saldo = row[0]

            # Calculate new balance
            if tipo == 'c':
                new_saldo = current_saldo + int(valor)
            else:  # 'd'
                new_saldo = current_saldo - int(valor)
                if new_saldo < -clientes[client_id]:
                    return "Limite ultrapassado!", 422

            # Call the stored function InsertTransacao.
            cur.execute("SELECT InsertTransacao(%s, %s, %s, %s)", (client_id, valor, tipo, descricao))
            updated_saldo_row = cur.fetchone()
            if updated_saldo_row is None:
                return "Database error inserting transaction", 500
            updated_saldo = updated_saldo_row[0]
            conn.commit()
            cliente_dto = {
                "limite": clientes[client_id],
                "saldo": updated_saldo
            }
            return jsonify(cliente_dto)
    except Exception as e:
        conn.rollback()
        return f"Database error inserting transaction: {e}", 500
    finally:
        pool.putconn(conn)

def is_transacao_valid(valor, tipo, descricao):
    # Value must be a positive integer
    if not isinstance(valor, int) and not (isinstance(valor, float) and valor.is_integer()):
        return False
    if valor <= 0:
        return False
    
    # Type must be either 'c' or 'd'
    if tipo not in ['c', 'd']:
        return False
    
    # Description must be non-empty and at most 10 characters
    if not descricao or not isinstance(descricao, str) or len(descricao) > 10:
        return False
    
    return True

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)