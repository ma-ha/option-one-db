# User Management REST API Reference

## GET /admin/user

Get users.

Authorization: Admin role

## POST /admin/user

Add user.

Authorization: Admin role

## POST /admin/user/:user/autz

Add user authorization.

Parameters:
- user: User-Id

Authorization: Admin role

## POST /admin/user/:user/password

Change users password.

Parameters:
- user: User-Id

Authorization: Admin role

## DELETE /admin/user/:user/autz

Remove user authorization.

Parameters:
- user: User-Id

Authorization: Admin role

## DELETE /admin/user

Delete user.

Authorization: Admin role
