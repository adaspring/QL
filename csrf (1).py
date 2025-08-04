from flask_wtf.csrf import CSRFProtect, generate_csrf

csrf = CSRFProtect()

def init_csrf(app):
    csrf.init_app(app)
    app.config['WTF_CSRF_TIME_LIMIT'] = 3600
    app.jinja_env.globals['csrf_token'] = generate_csrf