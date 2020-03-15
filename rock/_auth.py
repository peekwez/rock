import bcrypt
from branca import Branca


class TokenManager(object):
    _clients = {}

    def __init__(self, secrets):
        for key in secrets:
            self._clients[key] = Branca(secrets[key])

    def create(self, key, uid):
        return self._clients[key].encode(str(uid))

    def verify(self, key, token, ttl=7200):
        uid = self._clients[key].decode(token, ttl)
        return int(uid)


class PasswordManager(object):

    def encrypt(self, password, salt=None):
        if isinstance(password, str):
            password = password.encode('utf-8')
        if not salt:
            salt = bcrypt.gensalt(6)
        hashed = bcrypt.hashpw(password, salt)
        return salt, hashed
