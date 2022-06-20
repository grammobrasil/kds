import os


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'grammo%$!&&O'
    atlas_access = 'mongodb+srv://grammo:9laduLjBihz9x1SL@cluster0.vousz.mongodb.net/grammo' # noqa
    bubble_API_KEY = '0f73605fae6b5cfd879616aedb7c178c'
    MP_API_KEY = 'Bearer APP_USR-4016354548173801-071519-13c6571d089371a2cff6d938331a2473-740900544' # noqa
    senha_certificado = 'IpirAnga2805'
    CSC_homologacao = 'A9FA6834-7F13-40BC-9C8F-5CA3615FAA7C'
