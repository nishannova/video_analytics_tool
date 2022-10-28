# """Initialize Flask app."""
# from flask import Flask
# from flask_sqlalchemy import SQLAlchemy

# db = SQLAlchemy()


# def create_app():
#     """Construct the core application."""
#     app = Flask(__name__, static_folder='static')
#     UPLOAD_FOLDER = 'static/uploads'
#     app.secret_key = "secret key"
#     app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
#     app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
#     app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///project.db"

#     db.init_app(app)

#     with app.app_context():
#         from . import app  # Import routes

#         db.create_all()  # Create database tables for our data models

#         return app