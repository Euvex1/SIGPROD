from flask import Flask
from flask_cors import CORS
from routes import register_routes

# Criar a aplicação Flask
app = Flask(__name__)
CORS(app)

# Registrar todas as rotas
register_routes(app)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)

