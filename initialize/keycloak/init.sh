#!/bin/bash

### Added VM's / server's IP ADDRESS
IP_ADDRESS="10.0.1.65"
### Added VM's / server's keycloak listening port (8080 for non-ssl/8443 for ssl)
KEYCLOAK_LISTEN_PORT="8080"
### Add the master realm admin username for keycloak
MASTER_ADMIN_USER="admin"
### Add the master realm admin password for keycloak
MASTER_ADMIN_PASSWORD="admin"
### Change to "s" if ssl is available, else leave empty
SSL=""
### Leave realm name as is, if changed, `realm` field in realm.json  has to be changed accordingly
REALM="test_realm"

##########################################################################################################################################################################################################################
########################################################################################## DO NOT CHANGE ANYTHING FROM HERE ##############################################################################################
##########################################################################################################################################################################################################################

KEYCLOAK_BASE_URL="http${SSL}://${IP_ADDRESS}:${KEYCLOAK_LISTEN_PORT}/auth"

echo $KEYCLOAK_BASE_URL

# Fetch Keycloak admin token from master realm
results=$(curl -d "client_id=admin-cli" -d "username=${MASTER_ADMIN_USER}" -d "password=${MASTER_ADMIN_PASSWORD}"  -d 'grant_type=password' -s "${KEYCLOAK_BASE_URL}/realms/master/protocol/openid-connect/token" && echo)

token=$(./get_access_token.py "$results")

echo $token

# Create a realm
curl -ik -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" --data @realm.json -s "${KEYCLOAK_BASE_URL}/admin/realms/" && echo

# Create admin role in realm
curl -ik -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" --data @admin_role.json -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/roles" && echo

# Create user role in realm
curl -ik -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" --data @user_role.json -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/roles" && echo

# Create a client
curl -ik -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" --data @client.json -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/clients" && echo

# Fecth admin role id
admin_rid=$(./get_role_id.py "$(curl -X GET -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/roles" && echo)" "admin")
echo "admin_rid : $admin_rid"

# Create an admin user
curl -ik -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" --data @admin_user.json -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/users" && echo

# Fetch id of created admin user
uid=$(./get_id.py "$(curl -X GET -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/users?username=admin" && echo)")
echo $uid

echo "Mapping admin user to admin role"
# map admin user to admin role 
results=$(curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" -d "[{\"id\": \"${admin_rid}\", \"name\": \"admin\"}]" -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/users/${uid}/role-mappings/realm" && echo)
echo $results

# Fecth user role id
user_rid=$(./get_role_id.py "$(curl -X GET -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/roles" && echo)" "datasource_user")
echo "user_rid : $user_rid"

# map admin user to user role 
curl -ik -X POST -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: bearer $token" -d "[{\"id\": \"${user_rid}\", \"name\": \"datasource_user\"}]" -s "${KEYCLOAK_BASE_URL}/admin/realms/${REALM}/users/${uid}/role-mappings/realm" && echo


# Fetch client based token from new realm
results=$(curl -d 'client_id=bda_client' -d 'client_secret=bda_secret' -d 'grant_type=client_credentials' -H "Content-Type: application/x-www-form-urlencoded" -s "${KEYCLOAK_BASE_URL}/realms/${REALM}/protocol/openid-connect/token" && echo)
client_token=$(./get_access_token.py "$results")

# Get the id of Default Resource 
def_res=$(curl -X GET -s "${KEYCLOAK_BASE_URL}/realms/${REALM}/authz/protection/resource_set?name=Default%20Resource" -H "Authorization: Bearer $client_token" && echo)
def_res=$(echo $def_res | cut -d'"' -f2)

# Delete Default Resource from created client
curl -ik -X DELETE -s "${KEYCLOAK_BASE_URL}/realms/${REALM}/authz/protection/resource_set/$def_res" -H "Authorization: Bearer $client_token" && echo


