from flask import jsonify


# Bad request 400
def bad_request(message):
    response = jsonify({'status': 'Bad request', 'message': message})
    response.status_code = 400
    return response


# Unauthorized 401
def unauthorized(message):
    response = jsonify({'status': 'Unauthorized', 'message': message})
    response.status_code = 401
    return response


# Forbidden 403
def forbidden(message):
    response = jsonify({'status': 'Forbidden', 'message': message})
    response.status_code = 403
    return response


# Not found 404
def not_found(message):
    response = jsonify({'status': 'Not found', 'message': message})
    response.status_code = 404
    return response


# Internal Server error 500
def internal_server_error(message):
    response = jsonify({'status': 'Internal server error', 'message': message})
    response.status_code = 500
    return response


def message(m, e):
    return m, e, {'Access-Control-Allow-Origin': '*'}


def standard_response():
    return [
        {
            "code": 200,
            "message": "Success"
        },
        {
            "code": 400,
            "message": "Bad Request"
        },
        {
            "code": 500,
            "message": "Internal Server Error"
        }
    ]


def standard_delete_response():
    return [
        {
            "code": 204,
            "message": "Success"
        },
        {
            "code": 400,
            "message": "Bad Request"
        },
        {
            "code": 500,
            "message": "Internal Server Error"
        }
    ]


def standard_post_response():
    return [
        {
            "code": 201,
            "message": "Created"
        },
        {
            "code": 400,
            "message": "Bad Request"
        },
        {
            "code": 500,
            "message": "Internal Server Error"
        }
    ]

