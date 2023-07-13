import os
from flask import Flask, request, session, jsonify
from services import app as flask_api
# import app as flask_api
import logging
from loguru import logger
import jwt
from flask_cors import CORS

AUTH_FLAG=False

def validate(token):
    key = "SECURED_TRACE"
    if AUTH_FLAG:
        try:
            decoded_payload = jwt.decode(jwt=token, key=key, algorithms=['HS256'])
            logger.debug(f"Validated token - {decoded_payload}")
            return 1
        except Exception as e:
            logger.error(f"Failed to validate token - {e}")
            return 0
    else:
        return 1

def create_app(test_config=None):
    # create and configure the app
    # app = Flask(__name__,instance_relative_config=True)
    app = Flask(__name__, instance_relative_config=True, static_folder='/home/saintadmin/work/video_analytics_tool/services/static')

    CORS(app)
    app.config.from_mapping(SECRET_KEY='dev')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)


    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    
    
    logging.basicConfig(level=logging.DEBUG)
    
    app.register_blueprint(flask_api.bp)

    @app.before_request
    def jwt_validation():
        cookies = request.cookies
        if request.headers.get('Authorization'):
            access_token = request.headers['Authorization']
            access_token = str.replace(str(access_token), 'Bearer ', '')
        else:
            logger.debug(f"No token found in Authorization header")
            access_token = cookies.get("access_token")
            # access_token = str.replace(str(access_token), 'Bearer ', '')
        if request.headers.get('Id-Token'):
            session['id_token'] = request.headers.get('Id-Token')
        else:
            session['id_token'] = cookies.get("id_token")

        logger.info(f"Access token: {access_token}")
        logger.info(f"ID token: {session.get('id_token')}")

        session['access_token'] = access_token
        if validate(access_token) == 1:
            logger.info("JWT Token Validated")
        else:
            logger.error("Invalid Authorization: JWT authentication failed")
            return jsonify({"status": "FAILURE", "auth_error": 'Invalid Authorization'})

    return app

my_app = create_app()
# if __name__=="__main__":
#     app = create_app()
#     app.run(debug=True)    